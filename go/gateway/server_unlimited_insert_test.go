package gatewayd

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gatewaypb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/gatewaypb"
	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
)

type countingIngestPublisher struct {
	count int
}

func (p *countingIngestPublisher) Publish(_ context.Context, event *ingestpb.IngestEvent) error {
	if event.Key == nil || event.Content == "" {
		return fmt.Errorf("missing event content or key")
	}
	p.count++
	return nil
}

func TestInsertAcceptsMoreThanOldRateLimit(t *testing.T) {
	publisher := &countingIngestPublisher{}
	server := NewServer(nil, nil, publisher)

	for index := 0; index < 50; index++ {
		request := &gatewaypb.GatewayInsertRequest{
			Key: &gatewaypb.GatewayKey{
				ClientId: 42,
				Label:    uint64(index + 1),
			},
			Text:          "repository chunk",
			CorrelationId: fmt.Sprintf("chunk-%d", index),
		}
		response, err := server.Insert(context.Background(), request)
		if err != nil {
			t.Fatalf("insert %d failed: %v", index, err)
		}
		if response.CorrelationId != request.CorrelationId {
			t.Fatalf("insert %d correlation ID changed", index)
		}
	}

	if publisher.count != 50 {
		t.Fatalf("published %d events, want 50", publisher.count)
	}
}

func TestInsertStillRequiresVectorKey(t *testing.T) {
	server := NewServer(nil, nil, &countingIngestPublisher{})
	_, err := server.Insert(context.Background(), &gatewaypb.GatewayInsertRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Insert() code = %s, want InvalidArgument", status.Code(err))
	}
}
