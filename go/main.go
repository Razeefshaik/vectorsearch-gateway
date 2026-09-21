package main

import (
	"log"
	"net"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	gatewayd "github.com/Razeefshaik/vectorsearch-gateway/go/gateway"
	"github.com/Razeefshaik/vectorsearch-gateway/go/internal/config"
	"github.com/Razeefshaik/vectorsearch-gateway/go/internal/observability"
	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
	embedpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/embedpb"
	gatewaypb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/gatewaypb"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("warning: could not load .env file: %v", err)
	}

	coordinatorAddr := config.Getenv("COORDINATOR_ADDR", "localhost:50052")
	// EMBED_SEARCH_ADDR / EMBED_INGEST_ADDR let docker-compose point these at
	// container service names; local/hybrid dev only sets the *_PORT vars,
	// so it falls back to localhost:<port>.
	embedSearchAddr := config.AddrOrLocalPort("EMBED_SEARCH_ADDR", "EMBED_SEARCH_PORT")
	embedIngestAddr := config.AddrOrLocalPort("EMBED_INGEST_ADDR", "EMBED_INGEST_PORT")
	kafkaBroker := config.Getenv("KAFKA_BROKER", "localhost:9092")
	ingestTopic := config.Getenv("INGEST_TOPIC", "ingest-events")
	gatewaydPort := config.Getenv("GATEWAYD_PORT", "50053")
	metricsAddr := ":" + config.Getenv("GATEWAYD_METRICS_PORT", "9101")

	metricsSrv := observability.StartServer(metricsAddr, "gatewayd")
	defer observability.Shutdown(metricsSrv)

	coordinatorConn, err := grpc.NewClient(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial coordinator at %s: %v", coordinatorAddr, err)
	}
	coordinatorClient := coordinatorpb.NewVectorSearchClient(coordinatorConn)

	embedSearchConn, err := grpc.NewClient(embedSearchAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial embed search service at %s: %v", embedSearchAddr, err)
	}
	embedSearchClient := embedpb.NewEmbedServiceClient(embedSearchConn)

	embedIngestConn, err := grpc.NewClient(embedIngestAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial embed ingest service at %s: %v", embedIngestAddr, err)
	}
	_ = embedpb.NewEmbedServiceClient(embedIngestConn)

	producer := gatewayd.NewIngestProducer(kafkaBroker, ingestTopic)

	server := gatewayd.NewServer(coordinatorClient, embedSearchClient, producer)

	lis, err := net.Listen("tcp", ":"+gatewaydPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", gatewaydPort, err)
	}

	grpcMetrics := observability.NewGRPCServerMetrics("vsgw_gatewayd")
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(grpcMetrics.UnaryInterceptor()))
	gatewaypb.RegisterGatewayServer(grpcServer, server)

	log.Printf("gatewayd listening on :%s (metrics on %s)", gatewaydPort, metricsAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
