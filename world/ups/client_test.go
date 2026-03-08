package worldups

import (
	"log/slog"
	"mini-amazon-ups/proto"
	worldamazon "mini-amazon-ups/world/amazon"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
	protobuf "google.golang.org/protobuf/proto"
)

var worldClient *UpsWorldClient
var handler *UpsWorldHandler
var amazonWorldClient *worldamazon.AmazonWorldClient

var products []*proto.AProduct = []*proto.AProduct{
	{
		Id:          protobuf.Int64(1),
		Description: protobuf.String("Test Item"),
		Count:       protobuf.Int32(10),
	},
}

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		AddSource:  true,
		Level:      slog.LevelDebug,
		TimeFormat: time.Kitchen,
	})))

	godotenv.Load("../../ups/.env")
	host := os.Getenv("WORLD_SERVER_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("WORLD_SERVER_PORT")
	if port == "" {
		port = "12345"
	}

	var err error
	var worldID int64
	worldClient, worldID, err = NewUpsWorldClientAndConnect(host+":"+port, nil, nil)
	if err != nil {
		panic("Failed to create UPS world client: " + err.Error())
	}

	handler = NewDefaultUpsWorldHandler()
	slog.Info("Connected to world simulator with ID " + strconv.FormatInt(worldID, 10))

	amazonWorldClient, _, err = worldamazon.NewAmazonWorldClientAndConnect(host+":"+"23456", &worldID, nil)
	if err != nil {
		panic("Failed to create Amazon world client: " + err.Error())
	}
	preTestSetup()

	code := m.Run()

	// Cleanup
	worldClient.Disconnect()
	os.Exit(code)
}

func preTestSetup() {
	if !amazonWorldClient.IsConnected() {
		panic("Amazon world client is not connected")
	}
	if !worldClient.IsConnected() {
		panic("UPS world client is not connected")
	}

	worldClient.SetSimSpeed(200)

	amazonWorldClient.RequestPurchase(1, products)

	select {
	case resp := <-amazonWorldClient.ReceiveResponses():
		slog.Info("Successfully purchased items", "response", resp)
	case <-time.After(30 * time.Second):
		panic("Timed out waiting for purchase response")
	}
}

func waitForRequestPack(t *testing.T, warehouseID int32, packageID int64) {
	amazonWorldClient.RequestPack(warehouseID, products, packageID)
	select {
	case resq := <-amazonWorldClient.ReceiveResponses():
		slog.Info("Item Packed", "response", resq)
	case <-time.After(60 * time.Second):
		t.Fatal("Timed out waiting for pack response")
	}
}

func TestRequestPickup(t *testing.T) {
	const testTruckID = 1
	const testWarehouseID = 1
	const testPackageID = 12345

	slog.Info("Waiting for package to be packed")
	waitForRequestPack(t, testWarehouseID, testPackageID)

	seqNum, err := worldClient.RequestPickup(testTruckID, testWarehouseID)
	if err != nil {
		t.Fatal("Failed to request pickup: " + err.Error())
	}
	slog.Info("Requested pickup with sequence number " + strconv.FormatInt(seqNum, 10))

	select {
	case resp := <-worldClient.ReceiveResponses():
		slog.Info("Received response", "response", resp)

		// Check for completions
		if len(resp.Completions) > 0 {
			for _, completion := range resp.Completions {
				handler.HandleCompletion(completion)
				if completion.Seqnum != nil {
					worldClient.SendAck([]int64{*completion.Seqnum})
				}
			}
		}

		// Check for errors
		if len(resp.Error) > 0 {
			for _, errResp := range resp.Error {
				handler.HandleErr(errResp)
				if errResp.Originseqnum != nil && *errResp.Originseqnum == seqNum {
					t.Fatal("Received error for pickup request: " + errResp.GetErr())
				}
			}
		}

	case <-time.After(60 * time.Second):
		t.Fatal("Timed out waiting for response")
	}
}

func TestQueryTruck(t *testing.T) {
	const testTruckID = 1

	seqNum, err := worldClient.QueryTruck(testTruckID)
	if err != nil {
		t.Fatal("Failed to query truck: " + err.Error())
	}
	slog.Info("Queried truck with sequence number " + strconv.FormatInt(seqNum, 10))

	select {
	case resp := <-worldClient.ReceiveResponses():
		slog.Info("Received response", "response", resp)

		// Check for truck status
		if len(resp.Truckstatus) == 0 {
			t.Fatal("Expected truck status response, got " + resp.String())
		}

		for _, truckStatus := range resp.Truckstatus {
			handler.HandleTruckStatus(truckStatus)

			if truckStatus.Truckid == nil || *truckStatus.Truckid != testTruckID {
				t.Fatal("Expected truck status for truck " + strconv.FormatInt(int64(testTruckID), 10) + ", got " + resp.String())
			}

			if truckStatus.Seqnum != nil {
				worldClient.SendAck([]int64{*truckStatus.Seqnum})
			}
		}

		// Check for errors
		if len(resp.Error) > 0 {
			for _, errResp := range resp.Error {
				handler.HandleErr(errResp)
				if errResp.Originseqnum != nil && *errResp.Originseqnum == seqNum {
					t.Fatal("Received error for truck query: " + errResp.GetErr())
				}
			}
		}

	case <-time.After(60 * time.Second):
		t.Fatal("Timed out waiting for response")
	}
}

func waitForRequestLoad(t *testing.T, truckID int32, warehouseID int32, packageID int64) {
	amazonWorldClient.RequestLoad(warehouseID, truckID, packageID)
	select {
	case resq := <-amazonWorldClient.ReceiveResponses():
		slog.Info("Item Loaded", "response", resq)
	case <-time.After(120 * time.Second):
		t.Fatal("Timed out waiting for load response")
	}
}

func TestRequestDelivery(t *testing.T) {
	const testTruckID = 1
	const testWarehouseID = 1
	const testPackageID = 12345

	slog.Info("Waiting for package to be loaded")
	waitForRequestLoad(t, testTruckID, testWarehouseID, testPackageID)

	packages := []*proto.UDeliveryLocation{
		{
			Packageid: protobuf.Int64(testPackageID),
			X:         protobuf.Int32(50),
			Y:         protobuf.Int32(50),
		},
	}

	seqNum, err := worldClient.RequestDelivery(testTruckID, packages)
	if err != nil {
		t.Fatal("Failed to request delivery: " + err.Error())
	}
	slog.Info("Requested delivery with sequence number " + strconv.FormatInt(seqNum, 10))

	select {
	case resp := <-worldClient.ReceiveResponses():
		slog.Info("Received response", "response", resp)

		// Check for delivered messages
		if len(resp.Delivered) > 0 {
			for _, delivered := range resp.Delivered {
				handler.HandleDelivered(delivered)
				if delivered.Seqnum != nil {
					worldClient.SendAck([]int64{*delivered.Seqnum})
				}
			}
		}

		// Check for completions
		if len(resp.Completions) > 0 {
			for _, completion := range resp.Completions {
				handler.HandleCompletion(completion)
				if completion.Seqnum != nil {
					worldClient.SendAck([]int64{*completion.Seqnum})
				}
			}
		}

		// Check for errors
		if len(resp.Error) > 0 {
			for _, errResp := range resp.Error {
				handler.HandleErr(errResp)
				if errResp.Originseqnum != nil && *errResp.Originseqnum == seqNum {
					t.Fatal("Received error for delivery request: " + errResp.GetErr())
				}
			}
		}

	case <-time.After(60 * time.Second):
		t.Fatal("Timed out waiting for response")
	}
}
