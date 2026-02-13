package server

import (
	"mini-amazon-ups/proto"
	worldamazon "mini-amazon-ups/world/amazon"
)

type AmazonServer struct {
	proto.UnimplementedAmazonServiceServer
	WorldClient *worldamazon.AmazonWorldClient
}
