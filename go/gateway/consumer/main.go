package main

import (
	"context"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
	embedpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/embedpb"
	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("INGEST_TOPIC")
	group := os.Getenv("CONSUMER_GROUP")
	embedIngestAddr := "localhost:" + os.Getenv("EMBED_INGEST_PORT")
	coordinatorAddr := os.Getenv("COORDINATOR_ADDR")

	embedConn, err := grpc.NewClient(embedIngestAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial embed service: %v", err)
	}
	defer embedConn.Close()
	embedClient := embedpb.NewEmbedServiceClient(embedConn)

	coordConn, err := grpc.NewClient(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial coordinator: %v", err)
	}
	defer coordConn.Close()
	coordClient := coordinatorpb.NewVectorSearchClient(coordConn)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
		GroupID: group,
	})
	defer reader.Close()

	log.Println("consumer started, waiting for events...")

	for {
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Fatalf("failed to fetch message: %v", err)
		}

		if shouldCommit := processEvent(context.Background(), embedClient, coordClient, msg.Value); shouldCommit {
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				log.Printf("FAILED to commit offset for key=%s: %v", string(msg.Key), err)
			}
		}
	}
}

// processEvent embeds and inserts one event. It returns true if the Kafka
// offset should be committed -- either because the insert genuinely
// succeeded, or because the failure is permanent and retrying would never
// help (an unrecoverable event should not block every later message on this
// partition forever). It returns false only for transient failures, where
// leaving the offset uncommitted lets Kafka redeliver the event later.
func processEvent(ctx context.Context, embed embedpb.EmbedServiceClient, coord coordinatorpb.VectorSearchClient, raw []byte) bool {
	var event ingestpb.IngestEvent
	if err := proto.Unmarshal(raw, &event); err != nil {
		log.Printf("FAILED (permanent): could not unmarshal event: %v", err)
		return true
	}

	embedResp, err := embed.Embed(ctx, &embedpb.EmbedRequest{Text: event.Content})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.InvalidArgument {
			log.Printf("FAILED (permanent): empty/invalid content for client_id=%d label=%d: %v",
				event.Key.ClientId, event.Key.Label, err)
			return true
		}
		log.Printf("FAILED (transient): embed service error, will retry: %v", err)
		return false
	}

	_, err = coord.Insert(ctx, &coordinatorpb.InsertRequest{
		Key:    event.Key,
		Vector: embedResp.Vector,
	})
	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.AlreadyExists:
			log.Printf("OK (duplicate, treated as success): client_id=%d label=%d",
				event.Key.ClientId, event.Key.Label)
			return true
		case codes.ResourceExhausted, codes.InvalidArgument:
			log.Printf("FAILED (permanent): client_id=%d label=%d: %v",
				event.Key.ClientId, event.Key.Label, err)
			return true
		default:
			log.Printf("FAILED (transient): coordinator error, will retry: %v", err)
			return false
		}
	}

	log.Printf("OK: inserted client_id=%d label=%d", event.Key.ClientId, event.Key.Label)
	return true
}
