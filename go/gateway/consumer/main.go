package main

import (
	"context"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("warning: could not load .env file: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{os.Getenv("KAFKA_BROKER")},
		Topic:   os.Getenv("INGEST_TOPIC"),
		GroupID: os.Getenv("CONSUMER_GROUP"),
	})
	defer reader.Close()

	log.Println("consumer started, waiting for events...")

	for {
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Fatalf("failed to fetch message: %v", err)
		}

		log.Printf("received event: key=%s, value length=%d bytes", string(msg.Key), len(msg.Value))
	}
}
