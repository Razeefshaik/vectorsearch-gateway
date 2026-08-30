package gatewayd

import (
	"context"

	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
)

type Server struct {
	coordinatorpb.UnimplementedVectorSearchServer
	coordinator coordinatorpb.VectorSearchClient
}

func NewServer(coordinator coordinatorpb.VectorSearchClient) *Server {
	return &Server{coordinator: coordinator}
}

func (s *Server) Search(ctx context.Context, req *coordinatorpb.SearchRequest) (*coordinatorpb.SearchResponse, error) {
	return s.coordinator.Search(ctx, req)
}