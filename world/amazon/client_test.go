package worldamazon

import (
	"log/slog"
	"mini-amazon-ups/proto"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/joho/godotenv"
	protobuf "google.golang.org/protobuf/proto"
)

var worldClient *AmazonWorldClient
var handler *AmazonWorldHandler

func TestMain(m *testing.M) {
	godotenv.Load("../../amazon/.env")
	host := os.Getenv("WORLD_SERVER_HOST")
	if host == "" {
		host = "localhost"
	}
	port := "23456"

	var worldID int64
	var err error
	worldClient, worldID, err = NewAmazonWorldClientAndConnect(host+":"+port, nil, []*proto.AInitWarehouse{
		{
			Id: protobuf.Int32(1),
			X:  protobuf.Int32(10),
			Y:  protobuf.Int32(10),
		},
	})
	if err != nil {
		panic("Failed to connect to world simulator: " + err.Error())
	}

	handler = NewDefaultAmazonWorldHandler()
	slog.Info("Connected to world simulator with ID " + strconv.FormatInt(worldID, 10))
	m.Run()
}

func TestRequestPurchase(t *testing.T) {
	const testWarehouseNumber = 1
	const testProductDescription = "Test Product"
	const testProductId = 114514
	const testProductCount = 10

	seqNum, err := worldClient.RequestPurchase(testWarehouseNumber, []*proto.AProduct{
		{
			Id:          protobuf.Int64(testProductId),
			Description: protobuf.String(testProductDescription),
			Count:       protobuf.Int32(testProductCount),
		},
	})
	if err != nil {
		t.Fatal("Failed to request purchase: " + err.Error())
	}
	slog.Info("Requested purchase with sequence number " + strconv.FormatInt(seqNum, 10))
	select {
	case resp := <-worldClient.ReceiveResponses():
		slog.Info("Received response", "response", resp)
		if resp.Acks[len(resp.Acks)-1] != seqNum {
			t.Fatal("Expected ack for sequence number " + strconv.FormatInt(seqNum, 10) + ", got " + strconv.FormatInt(resp.Acks[len(resp.Acks)-1], 10))
		}
		if resp.Arrived == nil || len(resp.Arrived) != 1 {
			t.Fatal("Expected Arrived response, got " + resp.String())
		}
		arrived := resp.Arrived[len(resp.Arrived)-1]
		handler.purchaseMoreHandler = func(seqNum int64, whnum int32, things []*proto.AProduct) {
			if arrived.Whnum == nil || *arrived.Whnum != 1 {
				t.Fatal("Expected Arrived response for warehouse 1, got " + resp.String())
			}
			if arrived.Things == nil || len(arrived.Things) != 1 {
				t.Fatal("Expected Arrived response with 1 thing, got " + resp.String())
			}
			if arrived.Things[0].Id == nil || *arrived.Things[0].Id != testProductId {
				t.Fatal("Expected Arrived response with thing ID " + strconv.FormatInt(testProductId, 10) + ", got " + resp.String())
			}
			if arrived.Things[0].Count == nil || *arrived.Things[0].Count != testProductCount {
				t.Fatal("Expected Arrived response with thing count " + strconv.FormatInt(testProductCount, 10) + ", got " + resp.String())
			}
			if arrived.Things[0].Description == nil || *arrived.Things[0].Description != testProductDescription {
				t.Fatal("Expected Arrived response with thing description 'Test Product', got " + resp.String())
			}
			respSeqNum := arrived.Seqnum
			if respSeqNum == nil {
				t.Fatal("Expected Arrived response with sequence number, got " + resp.String())
			}
			worldClient.SendAck([]int64{seqNum})
		}
		handler.HandlePurchaseMore(arrived)
	case <-time.After(60 * time.Second):
		t.Fatal("Timed out waiting for response")
	}
}
