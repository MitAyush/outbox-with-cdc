package database

import (
	"context"
	"log"
	"os"

	"github.com/MitAyush/outbox-with-cdc/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func NewDBConnection(ctx context.Context, config *models.Config) *pgx.Conn {
	connStr := "postgres://" + config.DBUsername + ":" + config.DBPassword + "@" + config.DBHost + ":" + config.DBPort + "/" + config.DefaultDB
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatal("can not connect to database", err.Error())
	}

	return conn
}

// Regular connection: inspect the replication slot's progress.
func NewReplication(ctx context.Context, db *pgx.Conn) (*pgconn.PgConn, error) {
	replURL := os.Getenv("REPLICATION_URL")
	if replURL == "" {
		replURL = "postgres://cdc_user:cdc_password@localhost:5432/orders_db?replication=database&sslmode=disable"
	}
	// Replication connection: receive the logical change stream.
	return pgconn.Connect(ctx, replURL)

}
