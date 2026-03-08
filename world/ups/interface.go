package worldups

import "mini-amazon-ups/proto"

// WorldClientInterface defines the interface for communicating with the World simulator
type WorldClientInterface interface {
	Connect(worldID *int64, trucks []*proto.UInitTruck) (int64, error)
	RequestPickup(truckID int32, whID int32) (int64, error)
	RequestDelivery(truckID int32, packages []*proto.UDeliveryLocation) (int64, error)
	QueryTruck(truckID int32) (int64, error)
	SendAck(seqNums []int64) error
	ReceiveResponses() <-chan *proto.UResponses
}

// Ensure *UpsWorldClient implements WorldClientInterface at compile time
var _ WorldClientInterface = (*UpsWorldClient)(nil)
