package server

import (
	"context"
	"mini-amazon-ups/proto"
	worldups "mini-amazon-ups/world/ups"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	protobuf "google.golang.org/protobuf/proto"
)

// UpsServer implements the UpsService gRPC server
type UpsServer struct {
	proto.UnimplementedUpsServiceServer
	worldClient  *worldups.UpsWorldClient
	amazonClient proto.AmazonServiceClient
}

// NewUpsServer creates a new UpsServer instance with the given World client
func NewUpsServer(worldClient *worldups.UpsWorldClient) *UpsServer {
	return &UpsServer{
		worldClient:  worldClient,
		amazonClient: newAmazonClient(),
	}
}

// RequestPickup handles pickup requests from Amazon service
func (server *UpsServer) RequestPickup(ctx context.Context, req *proto.PickupRequest) (*proto.PickupResp, error) {
	return &proto.PickupResp{}, nil
}

// RequestRedirect handles redirect requests from Amazon service
func (server *UpsServer) RequestRedirect(ctx context.Context, req *proto.Redirect) (*proto.RedirectResp, error) {
	return &proto.RedirectResp{}, nil
}

// RequestCancel handles cancel requests from Amazon service
func (server *UpsServer) RequestCancel(ctx context.Context, req *proto.CancelOrder) (*proto.CancelResp, error) {
	return &proto.CancelResp{}, nil
}

// NotifyLoadReady handles load ready notifications from Amazon service
func (server *UpsServer) NotifyLoadReady(ctx context.Context, req *proto.LoadReady) (*proto.Ack, error) {
	return &proto.Ack{
		Success: *protobuf.Bool(true),
		Seqnum:  req.Seqnum,
	}, nil
}

// newAmazonClient creates a new gRPC client for communicating with the Amazon service
func newAmazonClient() proto.AmazonServiceClient {
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
