package server

import (
	"context"
	"log/slog"
	"mini-amazon-ups/proto"
	upsclient "mini-amazon-ups/ups/client"
	worldamazon "mini-amazon-ups/world/amazon"
	"net"
	"os"

	"google.golang.org/grpc"
	protobuf "google.golang.org/protobuf/proto"
)

// AmazonServer implements the AmazonService gRPC server
type AmazonServer struct {
	proto.UnimplementedAmazonServiceServer
	worldClient  *worldamazon.AmazonWorldClient
	upsClient    proto.UpsServiceClient
	worldHandler *worldamazon.AmazonWorldHandler
}

// NewAmazonServer creates a new AmazonServer instance with the given World client
func NewAmazonServer(worldClient *worldamazon.AmazonWorldClient) *AmazonServer {
	return &AmazonServer{
		worldClient:  worldClient,
		upsClient:    upsclient.NewUpsClient(),
		worldHandler: worldamazon.NewDefaultAmazonWorldHandler(),
	}
}

// Start starts the Amazon gRPC server and listens for incoming requests
func (server *AmazonServer) Start() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		panic("Failed to listen on port " + port)
	}
	slog.Info("[Amazon] Server starts to listen on port " + port)

	// Register gRPC server and start serving
	grpcServer := grpc.NewServer()
	proto.RegisterAmazonServiceServer(
		grpcServer,
		server,
	)
	grpcServer.Serve(lis)
}

// NotifyTruckArrived handles notifications from UPS service when a truck arrives at a warehouse
func (server *AmazonServer) NotifyTruckArrived(ctx context.Context, req *proto.TruckArrived) (*proto.Ack, error) {
	return &proto.Ack{
		Success: *protobuf.Bool(true),
		Seqnum:  req.Seqnum,
	}, nil
}

// NotifyDeliveryStarted handles notifications from UPS service when a delivery starts
func (server *AmazonServer) NotifyDeliveryStarted(ctx context.Context, req *proto.DeliveryStarted) (*proto.Ack, error) {
	return &proto.Ack{
		Success: *protobuf.Bool(true),
		Seqnum:  req.Seqnum,
	}, nil
}

// NotifyDeliveryComplete handles notifications from UPS service when a delivery is completed
func (server *AmazonServer) NotifyDeliveryComplete(ctx context.Context, req *proto.DeliveryComplete) (*proto.Ack, error) {
	return &proto.Ack{
		Success: *protobuf.Bool(true),
		Seqnum:  req.Seqnum,
	}, nil
}

// NotifyRedirectRequest handles notifications from UPS service when a redirect request is made
func (server *AmazonServer) NotifyRedirectRequest(ctx context.Context, req *proto.Redirect) (*proto.Ack, error) {
	return &proto.Ack{
		Success: *protobuf.Bool(true),
		Seqnum:  req.Seqnum,
	}, nil
}
