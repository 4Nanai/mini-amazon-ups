package server

import (
	"context"
	"mini-amazon-ups/proto"
	worldamazon "mini-amazon-ups/world/amazon"

	protobuf "google.golang.org/protobuf/proto"
)

type AmazonServer struct {
	proto.UnimplementedAmazonServiceServer
	WorldClient *worldamazon.AmazonWorldClient
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
