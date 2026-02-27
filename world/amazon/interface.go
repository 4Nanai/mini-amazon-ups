package worldamazon

import "mini-amazon-ups/proto"

// WorldClientInterface defines the interface for communicating with the World simulator
type WorldClientInterface interface {
	Connect(worldID *int64, warehouses []*proto.AInitWarehouse) (int64, error)
	RequestPack(whnum int32, products []*proto.AProduct, shipID int64) (int64, error)
	RequestLoad(whnum int32, truckID int32, shipID int64) (int64, error)
	RequestPurchase(whnum int32, products []*proto.AProduct) (int64, error)
	QueryPackage(packageID int64) (int64, error)
	SendAck(seqNums []int64) error
	ReceiveResponses() <-chan *proto.AResponses
}

// Ensure *AmazonWorldClient implements WorldClientInterface at compile time
var _ WorldClientInterface = (*AmazonWorldClient)(nil)
