package amazonclient

import (
	"mini-amazon-ups/proto"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// newAmazonClient creates a new gRPC client for communicating with the Amazon service
func NewAmazonClient() proto.AmazonServiceClient {
	host := os.Getenv("AMAZON_SERVER_HOST")
	if host == "" {
		host = "localhost" // Default host if not specified
	}
	port := os.Getenv("AMAZON_SERVER_PORT")
	if port == "" {
		port = "50051" // Default port if not specified
	}
	var err error
	conn, err := grpc.NewClient("dns:///"+host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return proto.NewAmazonServiceClient(conn)
}
