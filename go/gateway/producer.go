package gatewayd

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
)

var (
	kafkaPublishTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "vsgw_gatewayd",
		Subsystem: "kafka",
		Name:      "publish_total",
		Help:      "Ingest events published to Kafka, by result.",
	}, []string{"result"})

	kafkaPublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "vsgw_gatewayd",
		Subsystem: "kafka",
		Name:      "publish_duration_seconds",
		Help:      "Time to publish one ingest event to Kafka, in seconds.",
		Buckets:   prometheus.DefBuckets,
	})
)

func init() {
	prometheus.MustRegister(kafkaPublishTotal, kafkaPublishDuration)
}

type IngestProducer struct {
	writer *kafka.Writer
}

func NewIngestProducer(brokerAddr, topic string) *IngestProducer {

	return &IngestProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokerAddr),
			Topic:    topic,
			Balancer: &kafka.Hash{},
		},
	}
}

func (p *IngestProducer) Publish(ctx context.Context, event *ingestpb.IngestEvent) error {
	start := time.Now()
	value, err := proto.Marshal(event)
	if err != nil {
		kafkaPublishTotal.WithLabelValues("marshal_error").Inc()
		return err
	}
	key := []byte(clientIDKey(event.Key.ClientId))

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
	kafkaPublishDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		kafkaPublishTotal.WithLabelValues("error").Inc()
		return err
	}

	kafkaPublishTotal.WithLabelValues("success").Inc()
	return nil
}

func clientIDKey(clientID uint64) string {
	return strconv.FormatUint(clientID, 10)
}
