package gatewayd

import (
	"context"
	"strconv"

	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
	embedpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/embedpb"
	gatewaypb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/gatewaypb"
	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
	"github.com/Razeefshaik/vectorsearch-gateway/go/ratelimiter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	gatewaypb.UnimplementedGatewayServer
	coordinator coordinatorpb.VectorSearchClient
	embed       embedpb.EmbedServiceClient
	producer    *IngestProducer
	limiter     *ratelimiter.Limiter
}

func NewServer(coordinator coordinatorpb.VectorSearchClient, embed embedpb.EmbedServiceClient, producer *IngestProducer, limiter *ratelimiter.Limiter) *Server {
	return &Server{coordinator: coordinator, embed: embed, producer: producer, limiter: limiter}
}

func (s *Server) Search(ctx context.Context, req *gatewaypb.GatewaySearchRequest) (*gatewaypb.GatewaySearchResponse, error) {
	clientID := strconv.FormatUint(req.ClientId, 10)
	if !s.limiter.Allow(clientID) {
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	embedResp, err := s.embed.Embed(ctx, &embedpb.EmbedRequest{Text: req.Text})
	if err != nil {
		return nil, err
	}

	coordResp, err := s.coordinator.Search(ctx, &coordinatorpb.SearchRequest{
		Query:        embedResp.Vector,
		K:            req.K,
		Ef:           req.Ef,
		AllowPartial: req.AllowPartial,
		ClientId:     req.ClientId,
	})
	if err != nil {
		return nil, err
	}

	results := make([]*gatewaypb.GatewayScoredResult, len(coordResp.Results))
	for i, r := range coordResp.Results {
		results[i] = &gatewaypb.GatewayScoredResult{
			Key: &gatewaypb.GatewayKey{
				ClientId: r.Key.ClientId,
				Label:    r.Key.Label,
			},
			Distance: r.Distance,
		}
	}

	return &gatewaypb.GatewaySearchResponse{
		Results:       results,
		ShardsQueried: coordResp.ShardsQueried,
		ShardsFailed:  coordResp.ShardsFailed,
	}, nil
}

func (s *Server) Insert(ctx context.Context, req *gatewaypb.GatewayInsertRequest) (*gatewaypb.GatewayInsertResponse, error) {
	clientID := strconv.FormatUint(req.Key.ClientId, 10)
	if !s.limiter.Allow(clientID) {
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	event := &ingestpb.IngestEvent{
		Key: &coordinatorpb.Key{
			ClientId: req.Key.ClientId,
			Label:    req.Key.Label,
		},
		Content: req.Text,
	}

	if err := s.producer.Publish(ctx, event); err != nil {
		return nil, err
	}

	return &gatewaypb.GatewayInsertResponse{}, nil
}
