package main

import (
	"log"
	"net"
	"os"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	gatewayd "github.com/Razeefshaik/vectorsearch-gateway/go/gateway"
	coordinatorpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/coordinatorpb"
	embedpb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/embedpb"
	gatewaypb "github.com/Razeefshaik/vectorsearch-gateway/go/proto/gatewaypb"
	"github.com/Razeefshaik/vectorsearch-gateway/go/ratelimiter"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("warning: could not load .env file: %v", err)
	}

	coordinatorAddr := os.Getenv("COORDINATOR_ADDR")
	// .env only defines the embed services' ports; gatewayd runs on the host
	// alongside them, so the port is combined with localhost to form the addr.
	embedSearchAddr := "localhost:" + os.Getenv("EMBED_SEARCH_PORT")
	embedIngestAddr := "localhost:" + os.Getenv("EMBED_INGEST_PORT")
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	ingestTopic := os.Getenv("INGEST_TOPIC")
	gatewaydPort := os.Getenv("GATEWAYD_PORT")

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

	limiter := ratelimiter.NewLimiter(10, 1)
	limiter.StartCleanup(1*time.Minute, 10*time.Minute)

	server := gatewayd.NewServer(coordinatorClient, embedSearchClient, producer, limiter)

	lis, err := net.Listen("tcp", ":"+gatewaydPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", gatewaydPort, err)
	}

	grpcServer := grpc.NewServer()
	gatewaypb.RegisterGatewayServer(grpcServer, server)

	log.Printf("gatewayd listening on :%s", gatewaydPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
