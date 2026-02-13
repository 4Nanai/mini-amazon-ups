package server

import (
	"context"
	"mini-amazon-ups/proto"
	upsclient "mini-amazon-ups/ups/client"
	"testing"
	"time"
)

var upsClient proto.UpsServiceClient

func TestMain(m *testing.M) {
	upsServer := NewUpsServer(nil)
	go func() {
		upsServer.Start()
	}()
	time.Sleep(time.Second)
	upsClient = upsclient.NewUpsClient()
	m.Run()
}

func TestNotifyLoadReady(t *testing.T) {
	ctx := context.Background()

	seqNum := int64(12345)
	packageID := int64(67890)

	ack, err := upsClient.NotifyLoadReady(ctx, &proto.LoadReady{
		Seqnum:    seqNum,
		PackageId: packageID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Success {
		t.Fatalf("Expected success=true, got false")
	}
	if ack.Seqnum != seqNum {
		t.Fatalf("Expected seqnum=%d, got %d", seqNum, ack.Seqnum)
	}
	t.Logf("Received ack: success=%v, seqnum=%d", ack.Success, ack.Seqnum)
}

func TestRequestCancel(t *testing.T) {
	ctx := context.TODO()

	seqNum := int64(12345)
	packageID := int64(67890)

	resp, err := upsClient.RequestCancel(ctx, &proto.CancelOrder{
		Seqnum:    seqNum,
		PackageId: packageID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seqnum != seqNum {
		t.Fatalf("Expected seqnum=%d, got %d", seqNum, resp.Seqnum)
	}
	t.Logf("Received response: %v", resp)
}

func TestRequestPickup(t *testing.T) {
	ctx := context.TODO()

	seqNum := int64(12345)

	resp, err := upsClient.RequestPickup(ctx, &proto.PickupRequest{
		Seqnum: seqNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seqnum != seqNum {
		t.Fatalf("Expected seqnum=%d, got %d", seqNum, resp.Seqnum)
	}
	t.Logf("Received response: %v", resp)
}

func TestRequestRedirect(t *testing.T) {
	ctx := context.TODO()

	seqNum := int64(12345)
	packageID := int64(67890)

	resp, err := upsClient.RequestRedirect(ctx, &proto.Redirect{
		Seqnum:    seqNum,
		PackageId: packageID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Seqnum != seqNum {
		t.Fatalf("Expected seqnum=%d, got %d", seqNum, resp.Seqnum)
	}
	t.Logf("Received response: %v", resp)
}
