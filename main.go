package main

import (
	"context"

	"github.com/MitAyush/outbox-with-cdc/database"
	"github.com/MitAyush/outbox-with-cdc/processors"
	"github.com/MitAyush/outbox-with-cdc/cdc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	config := NewConfig()
	dbConn := database.NewDBConnection(ctx, config)

	for i := 0; i < config.WorkerCnt; i++ {
		// start consumer
		go func(i int) {
			consumer := NewConsumer(i, "order", "group-2")
			consumer.Start(processors.NewOrderProcessor(dbConn))
		}(i)
	}

	// start CDD

	cdc.StartCDC(ctx, dbConn)

	// to block main
	ch := make(chan bool)
	select {
	case <-ctx.Done():
		cancel()
		dbConn.Close(ctx)
	case <-ch:
		return
	}

}
