package server

import (
	"context"
	"mini-amazon-ups/proto"
	worldamazon "mini-amazon-ups/world/amazon"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	protobuf "google.golang.org/protobuf/proto"
)

// AmazonServer implements the AmazonService gRPC server
type AmazonServer struct {
	proto.UnimplementedAmazonServiceServer
	worldClient *worldamazon.AmazonWorldClient
	upsClient   proto.UpsServiceClient
}

// NewAmazonServer creates a new AmazonServer instance with the given World client
func NewAmazonServer(worldClient *worldamazon.AmazonWorldClient) *AmazonServer {
	return &AmazonServer{
		worldClient: worldClient,
		upsClient:   newUpsClient(),
	}
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

// newUpsClient creates a new gRPC client for communicating with the UPS service
func newUpsClient() proto.UpsServiceClient {
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
