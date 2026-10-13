package processors

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
)

type OrderDetails struct {
	OrderID   string
	UserID    string
	Amount    int
	Status    string
	CreatedAt time.Time
}

type OrderProcessor struct {
	DBConn *pgx.Conn
}

func NewOrderProcessor(conn *pgx.Conn) *OrderProcessor {
	return &OrderProcessor{
		DBConn: conn,
	}
}

func (p *OrderProcessor) Process(data []byte) error {
	// process order
	details := OrderDetails{}
	var err error
	err = json.Unmarshal(data, &details)
	if err != nil {
		return err
	}

	// write to database
	txnConfig := pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	}
	ctx := context.Background()
	txn, err := p.DBConn.BeginTx(ctx, txnConfig)
	defer func(){
		if err != nil {
			txn.Rollback(ctx)
		}
	}()
	if err != nil {
		return err
	}
	_, err = txn.Exec(ctx, "INSERT INTO orders (order_id, user_id, amount, status, created_at) VALUES ($1, $2, $3, $4, $5)",
		uuid.NewV4().String(), details.UserID, details.Amount, details.Status, details.CreatedAt)

	if err != nil{
		return err
	}
	txn.Commit(ctx)
	log.Println("done processing")
	return nil
}
