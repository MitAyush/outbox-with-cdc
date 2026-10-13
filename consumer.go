package main

import (
	"context"
	"log"

	"github.com/MitAyush/experiment1/processors"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	Reader *kafka.Reader
	ID     int
}

func NewConsumer(id int, topic, groupID string) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
		GroupID: groupID,
	})

	return &Consumer{
		Reader: reader,
		ID:     id,
	}
}

func (c *Consumer) Start(processor processors.Processor) {

	defer c.Reader.Close()
	log.Printf("Start consuming Kafka messages on topic %s with consumer group %s", c.Reader.Config().Topic, c.Reader.Config().GroupID)

	for {
		msg, err := c.Reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Failed to fetch message, %v", err)
			log.Fatal(err)
		}

		// process the msg
		log.Printf(
			"received key=%s value=%s partition=%d offset=%d ID=%d",
			string(msg.Key),
			string(msg.Value),
			msg.Partition,
			msg.Offset,
			c.ID,
		)

		err = processor.Process(msg.Value)
		if err != nil {
			log.Println("fail to process data: ", err)
		}

		// commit the msg
		if err = c.Reader.CommitMessages(context.Background(), msg); err != nil {
			log.Fatal(err)
		}
	}

}
