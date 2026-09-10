package main

import (
	"context"
	"log"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Razeefshaik/vectorsearch-gateway/go/internal/config"
	"github.com/Razeefshaik/vectorsearch-gateway/go/internal/observability"
	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
	embedpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/embedpb"
	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
)

var (
	messagesConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "vsgw_consumer",
		Name:      "messages_consumed_total",
		Help:      "Kafka messages fetched from the ingest topic.",
	})

	messagesProcessedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "vsgw_consumer",
		Name:      "messages_processed_total",
		Help:      "Processed ingest events, by final outcome.",
	}, []string{"outcome"}) // success | duplicate | permanent_error | transient_error

	processingDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "vsgw_consumer",
		Name:      "processing_duration_seconds",
		Help:      "End-to-end time to process one ingest event (embed + coordinator insert), in seconds.",
		Buckets:   prometheus.DefBuckets,
	})

	embedRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "vsgw_consumer",
		Subsystem: "embed_client",
		Name:      "requests_total",
		Help:      "Embed calls made by the consumer against embed-ingest, by result.",
	}, []string{"result"})

	coordinatorInsertTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "vsgw_consumer",
		Subsystem: "coordinator_client",
		Name:      "insert_requests_total",
		Help:      "Insert calls made by the consumer against the coordinator, by result.",
	}, []string{"result"})

	commitFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "vsgw_consumer",
		Name:      "commit_failures_total",
		Help:      "Kafka offset commits that failed after successful (or permanently-failed) processing.",
	})
)

func init() {
	prometheus.MustRegister(
		messagesConsumedTotal,
		messagesProcessedTotal,
		processingDuration,
		embedRequestsTotal,
		coordinatorInsertTotal,
		commitFailuresTotal,
	)
}

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Println("no .env file found, relying on real environment variables")
	}

	kafkaBroker := config.Getenv("KAFKA_BROKER", "localhost:9092")
	topic := config.Getenv("INGEST_TOPIC", "ingest-events")
	group := config.Getenv("CONSUMER_GROUP", "ingest-consumers")
	embedIngestAddr := config.AddrOrLocalPort("EMBED_INGEST_ADDR", "EMBED_INGEST_PORT")
	coordinatorAddr := config.Getenv("COORDINATOR_ADDR", "localhost:50052")
	metricsAddr := ":" + config.Getenv("CONSUMER_METRICS_PORT", "9102")

	metricsSrv := observability.StartServer(metricsAddr, "consumer")
	defer observability.Shutdown(metricsSrv)

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

	log.Printf("consumer started, waiting for events... (metrics on %s)", metricsAddr)

	for {
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Fatalf("failed to fetch message: %v", err)
		}
		messagesConsumedTotal.Inc()

		start := time.Now()
		shouldCommit := processEvent(context.Background(), embedClient, coordClient, msg.Value)
		processingDuration.Observe(time.Since(start).Seconds())

		if shouldCommit {
			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				commitFailuresTotal.Inc()
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
		messagesProcessedTotal.WithLabelValues("permanent_error").Inc()
		return true
	}

	embedResp, err := embed.Embed(ctx, &embedpb.EmbedRequest{Text: event.Content})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.InvalidArgument {
			embedRequestsTotal.WithLabelValues("invalid_argument").Inc()
			log.Printf("FAILED (permanent): empty/invalid content for client_id=%d label=%d: %v",
				event.Key.ClientId, event.Key.Label, err)
			messagesProcessedTotal.WithLabelValues("permanent_error").Inc()
			return true
		}
		embedRequestsTotal.WithLabelValues("error").Inc()
		log.Printf("FAILED (transient): embed service error, will retry: %v", err)
		messagesProcessedTotal.WithLabelValues("transient_error").Inc()
		return false
	}
	embedRequestsTotal.WithLabelValues("success").Inc()

	_, err = coord.Insert(ctx, &coordinatorpb.InsertRequest{
		Key:    event.Key,
		Vector: embedResp.Vector,
	})
	if err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.AlreadyExists:
			coordinatorInsertTotal.WithLabelValues("already_exists").Inc()
			log.Printf("OK (duplicate, treated as success): client_id=%d label=%d",
				event.Key.ClientId, event.Key.Label)
			messagesProcessedTotal.WithLabelValues("duplicate").Inc()
			return true
		case codes.ResourceExhausted, codes.InvalidArgument:
			coordinatorInsertTotal.WithLabelValues("permanent_error").Inc()
			log.Printf("FAILED (permanent): client_id=%d label=%d: %v",
				event.Key.ClientId, event.Key.Label, err)
			messagesProcessedTotal.WithLabelValues("permanent_error").Inc()
			return true
		default:
			coordinatorInsertTotal.WithLabelValues("transient_error").Inc()
			log.Printf("FAILED (transient): coordinator error, will retry: %v", err)
			messagesProcessedTotal.WithLabelValues("transient_error").Inc()
			return false
		}
	}

	coordinatorInsertTotal.WithLabelValues("success").Inc()
	messagesProcessedTotal.WithLabelValues("success").Inc()
	log.Printf("OK: inserted client_id=%d label=%d", event.Key.ClientId, event.Key.Label)
	return true
}
