package upsclient

import (
	"mini-amazon-ups/proto"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewUpsClient creates a new gRPC client for communicating with the UPS service
func NewUpsClient() proto.UpsServiceClient {
	host := os.Getenv("UPS_SERVER_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("UPS_SERVER_PORT")
	if port == "" {
		port = "50052"
	}
	var err error
	conn, err := grpc.NewClient("dns:///"+host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return proto.NewUpsServiceClient(conn)
}
