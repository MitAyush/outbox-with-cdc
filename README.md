# PostgreSQL CDC with Go and Kafka

A hands-on project for learning how PostgreSQL logical replication works
and how to build a small Change Data Capture (CDC) reader in Go.

The project reads committed row changes from PostgreSQL's logical
replication stream, groups row changes by transaction, and is being
built toward publishing each committed transaction to Kafka.

## Architecture

``` text
Application / SQL
       |
       v
  PostgreSQL orders
       |
       | WAL + logical decoding (pgoutput)
       v
 Go CDC reader (pglogrepl)
       |
       | Current: decode + buffer + log
       | Planned: one Kafka message per committed transaction
       v
 Kafka topic: order-events
```

The project also has an existing Kafka-to-PostgreSQL flow. Keep its
input topic (`order`) separate from the CDC output topic
(`order-events`) to avoid creating a feedback loop.

## Technology

-   Go
-   PostgreSQL 17
-   PostgreSQL logical replication using the `pgoutput` output plugin
-   `pgx` and `pglogrepl`
-   Kafka
-   `github.com/segmentio/kafka-go` for Kafka integration where used by
    the application

## Current status

Implemented in the CDC reader:

-   Opens a PostgreSQL replication-protocol connection.
-   Creates the logical replication slot if it does not exist.
-   Starts replication using publication `order_publication` and slot
    `orders_cdc`.
-   Parses replication protocol messages, including keepalives and WAL
    data.
-   Caches `RELATION` metadata so row values can be mapped to column
    names.
-   Handles `BEGIN`, `INSERT`, and `COMMIT` messages.
-   Accumulates decoded `INSERT` events in a single transaction buffer.
-   On `COMMIT`, logs the buffered transaction and clears the buffer.
-   Sends periodic standby status updates and replies when a keepalive
    requests a response.

Not implemented yet:

-   Publishing the buffered transaction to Kafka from the CDC reader.
-   Handling `UPDATE`, `DELETE`, and other row-change types.
-   Advancing replication progress only after Kafka confirms a
    successful write.
-   Durable deduplication / idempotency for replayed transactions.
-   High availability with an active and standby CDC instance.

**Important:** Until Kafka publishing and safe progress tracking are
implemented, this is a learning project, not a production-ready CDC
pipeline. Logging a transaction is not the same as durably processing
it.

## Configuration

The development setup discussed for this project uses:

  Setting                        Value
  ------------------------------ -----------------------
  PostgreSQL container           `experiment-postgres`
  PostgreSQL port                `5432`
  Database                       `orders_db`
  Application database user      `experiment`
  Publication                    `order_publication`
  Logical replication slot       `orders_cdc`
  Output plugin                  `pgoutput`
  Logical replication protocol   Version `1`
  Kafka input topic              `order`
  Planned CDC output topic       `order-events`

Use your local credentials and broker configuration; do not commit real
passwords or secrets to source control.

## PostgreSQL setup

### 1. Enable logical replication

PostgreSQL must have logical WAL enabled. For a local Docker-based
development instance, connect to the database and run:

``` sql
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
```

Restart PostgreSQL after changing `wal_level` (and restart after the
other settings as appropriate for your setup). Verify:

``` sql
SHOW wal_level;
SHOW max_wal_senders;
SHOW max_replication_slots;
```

Expected `wal_level`:

``` text
logical
```

### 2. Create the orders table

The table used in the current experiment has these columns. If the table
already exists, keep your existing schema instead of recreating it.

``` sql
CREATE TABLE IF NOT EXISTS public.orders (
    order_id   BIGINT PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    amount     NUMERIC(12, 2) NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3. Create the publication

``` sql
CREATE PUBLICATION order_publication
FOR TABLE public.orders;
```

If the publication already exists, inspect it rather than running the
creation statement again:

``` sql
SELECT pubname, puballtables
FROM pg_publication
WHERE pubname = 'order_publication';

SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'order_publication';
```

### 4. Replication permissions

The role used by the replication-protocol connection must have the
PostgreSQL privileges required for logical replication, including the
`REPLICATION` attribute and permission to connect to the target
database. The application also needs the appropriate permissions to
inspect slot metadata through its regular SQL connection.

Configure these roles and `pg_hba.conf` rules according to your local
Docker setup. Avoid using a superuser for an application in production.

The CDC reader creates the `orders_cdc` logical replication slot if it
does not exist. It uses the `pgoutput` plugin and the
`order_publication` publication.

## Running the CDC reader

Run the Go application using the repository's normal command and
environment configuration. The CDC code needs:

-   A regular PostgreSQL connection for inspecting replication-slot
    metadata.
-   A separate replication-protocol connection for streaming logical
    changes.
-   PostgreSQL connection settings and credentials.
-   A running Kafka broker when CDC-to-Kafka publishing is added.

The exact application entry point and connection environment variable
names depend on the repository's current configuration.

## Try it: create two rows in one transaction

Run the following SQL while the CDC reader is running:

``` sql
BEGIN;

INSERT INTO public.orders (order_id, user_id, amount, status, created_at)
VALUES (103, 10, 500.00, 'CREATED', NOW());

INSERT INTO public.orders (order_id, user_id, amount, status, created_at)
VALUES (104, 20, 800.00, 'CREATED', NOW());

COMMIT;
```

The CDC reader should log a `BEGIN`, accumulate two `INSERT` events, and
then log a `COMMIT` with `events=2`. It should clear the in-memory
transaction buffer after logging the transaction.

## Concepts explored

-   Write-Ahead Log (WAL)
-   Logical decoding and the `pgoutput` plugin
-   Publications and replication slots
-   Relation metadata and row decoding
-   Transaction boundaries (`BEGIN` and `COMMIT`)
-   Log Sequence Numbers (LSNs)
-   Standby status updates, keepalives, and replication progress
-   At-least-once delivery and duplicate handling
-   Kafka publishing and consumer scaling (planned)

### Acknowledgement safety

Receiving a replication message does not mean its changes have been
durably processed. The intended ordering is:

1.  Read and decode a PostgreSQL transaction.
2.  Publish the complete transaction to Kafka.
3.  Wait for Kafka to confirm the write.
4.  Advance the safe replication position and send a standby status
    update.

If the CDC process crashes after Kafka accepts a message but before
PostgreSQL receives the progress update, the transaction may be
published again. Downstream idempotency or deduplication will be needed.
Do not acknowledge progress merely because events were logged.

## Next steps

1.  Add Kafka publishing at the transaction `COMMIT` boundary.
2.  Advance the acknowledged LSN only after Kafka confirms the
    transaction message.
3.  Add `UPDATE` and `DELETE` decoding.
4.  Test failures and replay behavior.
5.  Consider an active/standby CDC deployment only after the
    single-reader pipeline is correct.

## Learning approach

This repository is intended as a practical learning exercise. The goal
is to understand the protocol and failure semantics by building the
pipeline incrementally, rather than implementing every production
feature at once.
