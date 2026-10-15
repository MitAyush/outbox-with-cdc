package cdc

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/MitAyush/outbox-with-cdc/database"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	slotName        = "orders_cdc"
	publication     = "order_publication"
	replicationDB   = "orders_db"
	heatbeatTimeout = 30
)

type TransactionBuffer struct {
	XID    uint32
	Events []CDCEvent
}

type CDCEvent struct {
	Type  string         `json:"type"`
	Table string         `json:"table"`
	Data  map[string]any `json:"data"`
}

func StartCDC(ctx context.Context, db *pgx.Conn) {
	replConn, err := database.NewReplication(ctx, db)

	if err != nil {
		log.Fatalf("connect using replication protocol: %v", err)
	}
	defer replConn.Close(ctx)

	startLSN, slotExists, err := getSlotPosition(ctx, db)
	if err != nil {
		log.Fatalf("inspect replication slot: %v", err)
	}

	if !slotExists {
		result, err := pglogrepl.CreateReplicationSlot(
			ctx,
			replConn,
			slotName,
			"pgoutput",
			pglogrepl.CreateReplicationSlotOptions{
				Mode: pglogrepl.LogicalReplication,
			},
		)
		if err != nil {
			log.Fatalf("create replication slot: %v", err)
		}

		startLSN, err = pglogrepl.ParseLSN(result.ConsistentPoint)
		if err != nil {
			log.Fatalf("parse consistent point: %v", err)
		}

		log.Printf(
			"Created slot %s at consistent point %s",
			slotName,
			startLSN,
		)
	} else {
		log.Printf(
			"Resuming slot %s from LSN %s",
			slotName,
			startLSN,
		)
	}

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", publication),
	}

	err = pglogrepl.StartReplication(
		ctx,
		replConn,
		slotName,
		startLSN,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: pluginArgs,
		},
	)
	if err != nil {
		log.Fatalf("start logical replication: %v", err)
	}

	log.Printf(
		"CDC reader started: database=%s publication=%s slot=%s",
		replicationDB,
		publication,
		slotName,
	)

	// Relation messages map PostgreSQL relation IDs to table metadata.
	relations := make(map[uint32]*pglogrepl.RelationMessage)

	// Keep only the transaction currently being decoded. With pgoutput
	// protocol version 1, we don't use streaming of in-progress transactions.
	var currentTxn *TransactionBuffer

	// Until Kafka publishing is implemented, keep reporting only the
	// starting position. Logging a transaction is not durable processing.
	lastAckLSN := startLSN
	nextHeartbeat := time.Now().Add(heatbeatTimeout * time.Second)

	for {
		if time.Now().After(nextHeartbeat) {
			// err = pglogrepl.SendStandbyStatusUpdate(
			// 	ctx,
			// 	replConn,
			// 	pglogrepl.StandbyStatusUpdate{
			// 		WALWritePosition: lastAckLSN,
			// 		WALFlushPosition: lastAckLSN,
			// 		WALApplyPosition: lastAckLSN,
			// 	},
			// )
			// if err != nil {
			// 	log.Fatalf("send standby status: %v", err)
			// }
			// log.Println("sending hearbeat")

			nextHeartbeat = time.Now().Add(heatbeatTimeout * time.Second)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextHeartbeat)
		rawMsg, err := replConn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				log.Println("timout...")
				continue
			}

			log.Fatalf("receive replication message: %v", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.ErrorResponse:
			log.Fatalf("PostgreSQL replication error: %s", msg.Message)

		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(
					msg.Data[1:],
				)
				if err != nil {
					log.Fatalf("parse keepalive: %v", err)
				}

				if keepalive.ReplyRequested {
					// Reply without acknowledging changes beyond
					// the starting LSN.
					sendStatus(ctx, lastAckLSN, replConn)
					log.Println("replying to keep alive msg")
				}

			case pglogrepl.XLogDataByteID:
				xlogData, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalf("parse XLogData: %v", err)
				}

				logicalMsg, err := pglogrepl.Parse(xlogData.WALData)
				if err != nil {
					log.Fatalf("parse logical message: %v", err)
				}

				switch m := logicalMsg.(type) {
				case *pglogrepl.RelationMessage:
					relations[m.RelationID] = m

					log.Printf(
						"RELATION: %s.%s id=%d columns=%d",
						m.Namespace,
						m.RelationName,
						m.RelationID,
						len(m.Columns),
					)

				case *pglogrepl.BeginMessage:
					if currentTxn != nil {
						log.Fatalf("received BEGIN for xid=%d while xid=%d is still active", m.Xid, currentTxn.XID)
					}

					currentTxn = &TransactionBuffer{
						XID:    m.Xid,
						Events: make([]CDCEvent, 0),
					}

					log.Printf(
						"BEGIN: xid=%d final_lsn=%s",
						m.Xid,
						m.FinalLSN,
					)

				case *pglogrepl.InsertMessage:
					rel, ok := relations[m.RelationID]
					if !ok {
						log.Fatalf(
							"unknown relation ID %d",
							m.RelationID,
						)
					}

					values := make(map[string]any)

					for i, col := range m.Tuple.Columns {
						if i >= len(rel.Columns) {
							log.Fatalf("column metadata mismatch")
						}

						name := rel.Columns[i].Name

						switch col.DataType {
						case 'n':
							values[name] = nil
						case 't':
							values[name] = string(col.Data)
						case 'u':
							values[name] = "<unchanged TOAST value>"
						}
					}

					if currentTxn == nil {
						log.Fatalf("received INSERT for relation %s.%s outside an active transaction", rel.Namespace, rel.RelationName)
					}

					// Accumulate the decoded row change; don't publish or acknowledge it yet.
					currentTxn.Events = append(currentTxn.Events, CDCEvent{
						Type:  "INSERT",
						Table: rel.Namespace + "." + rel.RelationName,
						Data:  values,
					})

					log.Printf(
						"ACCUMULATED INSERT: xid=%d table=%s.%s transaction_events=%d values=%+v",
						currentTxn.XID,
						rel.Namespace,
						rel.RelationName,
						len(currentTxn.Events),
						values,
					)

				case *pglogrepl.CommitMessage:
					if currentTxn == nil {
						log.Fatalf("received COMMIT without an active transaction")
					}

					log.Printf(
						"COMMIT: xid=%d commit_lsn=%s end_lsn=%s commit_time=%s events=%d",
						currentTxn.XID,
						m.CommitLSN,
						m.TransactionEndLSN,
						m.CommitTime,
						len(currentTxn.Events),
					)

					// For now, log the whole transaction as one batch. The next step
					// will replace this with a Kafka write. Do NOT advance lastAckLSN
					// until Kafka confirms the transaction was durably written.
					for i, event := range currentTxn.Events {
						log.Printf("TRANSACTION EVENT %d/%d: type=%s table=%s data=%+v",
							i+1, len(currentTxn.Events), event.Type, event.Table, event.Data)
					}

					log.Printf("TRANSACTION BUFFER CLEARED: xid=%d", currentTxn.XID)
					currentTxn = nil
					lastAckLSN = m.TransactionEndLSN
					sendStatus(ctx, lastAckLSN, replConn)

				default:
					// Log other message types we haven't implemented yet.
					log.Printf(
						"LOGICAL MESSAGE: type=%s",
						logicalMsg.Type(),
					)
				}

			default:
				log.Printf(
					"Unknown replication message type: %q",
					msg.Data[0],
				)
			}

		default:
			log.Printf("Unexpected PostgreSQL message: %T", rawMsg)
		}
	}
}
