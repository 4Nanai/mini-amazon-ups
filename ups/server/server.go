package server

import (
	"context"
	"mini-amazon-ups/proto"
	worldups "mini-amazon-ups/world/ups"

	protobuf "google.golang.org/protobuf/proto"
)

type UpsServer struct {
	proto.UnimplementedUpsServiceServer
	WorldClient *worldups.UpsWorldClient
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
