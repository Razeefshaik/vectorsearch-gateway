package gatewayd

import (
	"context"
	"testing"

	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
	gatewaypb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/gatewaypb"
	"github.com/Razeefshaik/vectorsearch-gateway/go/ratelimiter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type deleteTestCoordinator struct {
	deleteError error
}

func (c *deleteTestCoordinator) Insert(context.Context, *coordinatorpb.InsertRequest, ...grpc.CallOption) (*coordinatorpb.InsertResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (c *deleteTestCoordinator) Delete(context.Context, *coordinatorpb.DeleteRequest, ...grpc.CallOption) (*coordinatorpb.DeleteResponse, error) {
	if c.deleteError != nil {
		return nil, c.deleteError
	}
	return &coordinatorpb.DeleteResponse{}, nil
}

func (c *deleteTestCoordinator) Search(context.Context, *coordinatorpb.SearchRequest, ...grpc.CallOption) (*coordinatorpb.SearchResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func TestDeleteTreatsMissingVectorAsSuccessfulRetry(t *testing.T) {
	server := NewServer(
		&deleteTestCoordinator{
			deleteError: status.Error(codes.NotFound, "missing"),
		},
		nil,
		nil,
		ratelimiter.NewLimiter(10, 1),
	)

	response, err := server.Delete(context.Background(), &gatewaypb.GatewayDeleteRequest{
		Key: &gatewaypb.GatewayKey{
			ClientId: 42,
			Label:    99,
		},
		CorrelationId: "cleanup-1",
	})

	if err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}
	if response.CorrelationId != "cleanup-1" {
		t.Fatalf("Delete() correlation ID = %q, want cleanup-1", response.CorrelationId)
	}
}
