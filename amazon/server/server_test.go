package server

import (
	"context"
	amazonclient "mini-amazon-ups/amazon/client"
	"mini-amazon-ups/proto"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

var amazonClient proto.AmazonServiceClient

func TestMain(m *testing.M) {
	godotenv.Load("../.env")
	worldHost := os.Getenv("WORLD_SERVER_HOST")
	if worldHost == "" {
		worldHost = "localhost"
	}
	worldPort := os.Getenv("WORLD_SERVER_PORT")
	if worldPort == "" {
		worldPort = "23456"
	}
	amazonServer := AmazonServer{}
	go func() {
		amazonServer.StartServe()
	}()
	time.Sleep(time.Second)
	amazonClient = amazonclient.NewAmazonClient()
	m.Run()
}

func TestNotifyDeliveryComplete(t *testing.T) {
	ctx := context.Background()

	seqNum := int64(12345)

	ack, err := amazonClient.NotifyDeliveryComplete(ctx, &proto.DeliveryComplete{
		Seqnum: seqNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Success {
		t.Fatalf("expected success, got failure")
	}
	if ack.Seqnum != seqNum {
		t.Fatalf("expected seqnum %d, got %d", seqNum, ack.Seqnum)
	}
}

func TestNotifyDeliveryStarted(t *testing.T) {
	ctx := context.Background()

	seqNum := int64(12345)

	ack, err := amazonClient.NotifyDeliveryStarted(ctx, &proto.DeliveryStarted{
		Seqnum: seqNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Success {
		t.Fatalf("expected success, got failure")
	}
	if ack.Seqnum != seqNum {
		t.Fatalf("expected seqnum %d, got %d", seqNum, ack.Seqnum)
	}
}

func TestNotifyRedirectRequest(t *testing.T) {
	ctx := context.Background()

	seqNum := int64(12345)

	ack, err := amazonClient.NotifyRedirectRequest(ctx, &proto.Redirect{
		Seqnum: seqNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Success {
		t.Fatalf("expected success, got failure")
	}
	if ack.Seqnum != seqNum {
		t.Fatalf("expected seqnum %d, got %d", seqNum, ack.Seqnum)
	}
}

func TestNotifyTruckArrived(t *testing.T) {
	ctx := context.Background()

	seqNum := int64(12345)

	ack, err := amazonClient.NotifyTruckArrived(ctx, &proto.TruckArrived{
		Seqnum: seqNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Success {
		t.Fatalf("expected success, got failure")
	}
	if ack.Seqnum != seqNum {
		t.Fatalf("expected seqnum %d, got %d", seqNum, ack.Seqnum)
	}
}
