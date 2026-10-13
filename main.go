package main

import (
	"context"

	"github.com/MitAyush/experiment1/db"
	"github.com/MitAyush/experiment1/processors"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	config := NewConfig()
	dbConn := db.NewDBConnection(ctx, config)


	for i := 0; i < config.WorkerCnt; i++ {
		// start consumer
		go func(i int) {
			consumer := NewConsumer(i, "order", "group-2")
			consumer.Start(processors.NewOrderProcessor(dbConn))
		}(i)
	}


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
