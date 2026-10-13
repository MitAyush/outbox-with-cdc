package db

import (
	"context"
	"log"

	"github.com/MitAyush/experiment1/models"
	"github.com/jackc/pgx/v5"
)



func NewDBConnection(ctx context.Context, config *models.Config) *pgx.Conn {
	connStr := "postgres://" + config.DBUsername + ":" + config.DBPassword + "@" + config.DBHost + ":" + config.DBPort + "/" + config.DefaultDB
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatal("can not connect to database", err.Error())
	}
	
	return conn
}