package server

import (
	"mini-amazon-ups/proto"
	worldups "mini-amazon-ups/world/ups"
)

type UpsServer struct {
	proto.UnimplementedUpsServiceServer

	WorldClient *worldups.UpsWorldClient
}
