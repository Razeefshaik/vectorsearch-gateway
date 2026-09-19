package gatewayd

import (
	"context"
	"strconv"

	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
	embedpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/embedpb"
	gatewaypb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/gatewaypb"
	ingestpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/ingestpb"
	"github.com/Razeefshaik/vectorsearch-gateway/go/ratelimiter"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ratelimitDeniedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "vsgw_gatewayd",
	Subsystem: "ratelimiter",
	Name:      "denied_total",
	Help:      "Requests rejected by the per-client rate limiter, by RPC method.",
}, []string{"method"})

func init() {
	prometheus.MustRegister(ratelimitDeniedTotal)
}

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
		ratelimitDeniedTotal.WithLabelValues("Search").Inc()
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
		ratelimitDeniedTotal.WithLabelValues("Insert").Inc()
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	event := &ingestpb.IngestEvent{
		Key: &coordinatorpb.Key{
			ClientId: req.Key.ClientId,
			Label:    req.Key.Label,
		},
		Content:       req.Text,
		CorrelationId: req.CorrelationId,
	}

	if err := s.producer.Publish(ctx, event); err != nil {
		return nil, err
	}

	return &gatewaypb.GatewayInsertResponse{
		CorrelationId: req.CorrelationId,
	}, nil
}

// Delete synchronously removes one vector. A missing vector is treated as
// success so callers can safely retry cleanup after a timeout or crash.
func (s *Server) Delete(ctx context.Context, req *gatewaypb.GatewayDeleteRequest) (*gatewaypb.GatewayDeleteResponse, error) {
	if req == nil || req.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	clientID := strconv.FormatUint(req.Key.ClientId, 10)
	if !s.limiter.Allow(clientID) {
		ratelimitDeniedTotal.WithLabelValues("Delete").Inc()
		return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}

	_, err := s.coordinator.Delete(ctx, &coordinatorpb.DeleteRequest{
		Key: &coordinatorpb.Key{
			ClientId: req.Key.ClientId,
			Label:    req.Key.Label,
		},
	})
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, err
	}

	return &gatewaypb.GatewayDeleteResponse{
		CorrelationId: req.CorrelationId,
	}, nil
}
