package cdc

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Returns:
//   - LSN: The position selected from the slot's metadata.
//   - bool: true if the slot exists, false if it doesn't.
//   - error: Any database or LSN parsing error.
//
// Note: This function only inspects slot metadata. It does not create
// a slot or start reading WAL changes.
func getSlotPosition(ctx context.Context, db *pgx.Conn) (pglogrepl.LSN, bool, error) {
	// PostgreSQL stores these positions as LSN values.
	// We scan them as strings so we can handle NULL values easily.
	var confirmedLSN string
	var restartLSN string

	// pg_replication_slots is a PostgreSQL system view containing
	// metadata about replication slots.
	//
	// $1 is a parameter placeholder for slotName.
	// We only want a logical slot belonging to the current database.
	err := db.QueryRow(
		ctx,
		`
		SELECT
		    COALESCE(confirmed_flush_lsn::text, ''),
		    COALESCE(restart_lsn::text, '')
		FROM pg_replication_slots
		WHERE slot_name = $1
		  AND slot_type = 'logical'
		  AND database = current_database()
		`,
		slotName,
	).Scan(&confirmedLSN, &restartLSN)

	if err != nil {
		// QueryRow().Scan() returns pgx.ErrNoRows when no matching
		// replication slot exists.
		//
		// This isn't a database failure. The caller can use the
		// false result to decide whether to create the slot.
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}

		// Any other error indicates a problem, such as a failed query
		// or database connection issue.
		return 0, false, err
	}

	// confirmed_flush_lsn records the progress confirmed by the
	// logical replication consumer.
	//
	// restart_lsn indicates the oldest WAL position PostgreSQL may
	// still need to retain for this slot.
	//
	// IMPORTANT: These positions have different meanings.
	// Do not automatically treat restart_lsn as a safe resume point.
	// We'll implement the correct resume-position logic when we
	// start consuming logical replication changes.
	position := confirmedLSN

	if position == "" {
		// No confirmed position is available.
		// Keep restartLSN available for inspection, but don't assume
		// it is the correct logical decoding start position.
		return 0, true, fmt.Errorf(
			"slot exists but has no confirmed flush position; restart_lsn=%q",
			restartLSN,
		)
	}

	// Convert the textual LSN (for example, "0/16B6C50") into
	// pglogrepl's LSN type so it can be used by the replication API.
	lsn, err := pglogrepl.ParseLSN(position)
	if err != nil {
		return 0, true, fmt.Errorf(
			"parse confirmed_flush_lsn %q: %w",
			position,
			err,
		)
	}

	// The slot exists, and we successfully parsed its confirmed
	// replication position.
	return lsn, true, nil
}

func sendStatus(ctx context.Context, lastAckLSN pglogrepl.LSN, replConn *pgconn.PgConn) {
	err := pglogrepl.SendStandbyStatusUpdate(
		ctx,
		replConn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lastAckLSN,
			WALFlushPosition: lastAckLSN,
			WALApplyPosition: lastAckLSN,
		},
	)
	if err != nil {
		log.Fatalf("reply to keepalive: %v", err)
	}

}
