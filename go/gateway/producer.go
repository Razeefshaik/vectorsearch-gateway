package gatewayd

import (
	"context"
	"strconv"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
)

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
	value, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	key := []byte(clientIDKey(event.Key.ClientId))

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})

}

func clientIDKey(clientID uint64) string {
	return strconv.FormatUint(clientID, 10)
}
