package server

import (
	"context"
	"log/slog"
	amazonclient "mini-amazon-ups/amazon/client"
	"mini-amazon-ups/proto"
	worldups "mini-amazon-ups/world/ups"
	"net"
	"os"

	"google.golang.org/grpc"
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
		amazonClient: amazonclient.NewAmazonClient(),
	}
}

// Start starts the gRPC server to listen for incoming requests from Amazon service
func (server *UpsServer) Start() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "50052" // Default port if not specified
	}
	lis, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		panic("Error starting server")
	}
	slog.Info("[UPS] Server starts to listen on port " + port)
	grpcServer := grpc.NewServer()
	proto.RegisterUpsServiceServer(
		grpcServer,
		server,
	)
	grpcServer.Serve(lis)
}

// RequestPickup handles pickup requests from Amazon service
func (server *UpsServer) RequestPickup(ctx context.Context, req *proto.PickupRequest) (*proto.PickupResp, error) {
	return &proto.PickupResp{
		Seqnum: req.Seqnum,
	}, nil
}

// RequestRedirect handles redirect requests from Amazon service
func (server *UpsServer) RequestRedirect(ctx context.Context, req *proto.Redirect) (*proto.RedirectResp, error) {
	return &proto.RedirectResp{
		Seqnum:    req.Seqnum,
		PackageId: req.PackageId,
	}, nil
}

// RequestCancel handles cancel requests from Amazon service
func (server *UpsServer) RequestCancel(ctx context.Context, req *proto.CancelOrder) (*proto.CancelResp, error) {
	return &proto.CancelResp{
		Seqnum:    req.Seqnum,
		PackageId: req.PackageId,
	}, nil
}

// NotifyLoadReady handles load ready notifications from Amazon service
func (server *UpsServer) NotifyLoadReady(ctx context.Context, req *proto.LoadReady) (*proto.Ack, error) {
	return &proto.Ack{
		Success: *protobuf.Bool(true),
		Seqnum:  req.Seqnum,
	}, nil
}
