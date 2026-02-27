package server

import (
	"context"
	"fmt"
	amazonclient "mini-amazon-ups/amazon/client"
	"mini-amazon-ups/amazon/db"
	"mini-amazon-ups/proto"
	"sync"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

// mockDatabase is a simple in-memory database for testing
type mockDatabase struct {
	orders      map[int64]*db.Order
	ordersByPkg map[int64]*db.Order
	inventory   map[string]int // key: "warehouseID:productID"
	orderIDSeq  int64
	mu          sync.RWMutex
}

func newMockDatabase() *mockDatabase {
	return &mockDatabase{
		orders:      make(map[int64]*db.Order),
		ordersByPkg: make(map[int64]*db.Order),
		inventory:   make(map[string]int),
		orderIDSeq:  0,
	}
}

func (m *mockDatabase) CreateOrderWithItems(upsUserID string, destX, destY, warehouseID int, items map[int64]int) (*db.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.orderIDSeq++
	order := &db.Order{
		OrderID:     m.orderIDSeq,
		UpsUserID:   upsUserID,
		DestX:       destX,
		DestY:       destY,
		WarehouseID: warehouseID,
		Status:      "CREATED",
	}
	m.orders[order.OrderID] = order
	return order, nil
}

func (m *mockDatabase) GetOrder(orderID int64) (*db.Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	order, exists := m.orders[orderID]
	if !exists {
		return nil, fmt.Errorf("order not found")
	}
	return order, nil
}

func (m *mockDatabase) GetOrderByPackageID(packageID int64) (*db.Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	order, exists := m.ordersByPkg[packageID]
	if !exists {
		return nil, fmt.Errorf("order not found")
	}
	return order, nil
}

func (m *mockDatabase) UpdateOrderAfterPickup(orderID int64, packageID int64, truckID int, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	order, exists := m.orders[orderID]
	if !exists {
		return fmt.Errorf("order not found")
	}
	order.PackageID = &packageID
	order.TruckID = &truckID
	order.Status = status
	m.ordersByPkg[packageID] = order
	return nil
}

func (m *mockDatabase) UpdateOrderStatusByPackageID(packageID int64, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	order, exists := m.ordersByPkg[packageID]
	if !exists {
		// For testing, just create a dummy order if not found
		order = &db.Order{
			PackageID: &packageID,
			Status:    status,
		}
		m.ordersByPkg[packageID] = order
		return nil
	}
	order.Status = status
	return nil
}

func (m *mockDatabase) UpdateOrderDestination(orderID int64, destX, destY int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	order, exists := m.orders[orderID]
	if !exists {
		return fmt.Errorf("order not found")
	}
	order.DestX = destX
	order.DestY = destY
	return nil
}

func (m *mockDatabase) AddOrUpdateInventory(warehouseID int, productID int64, count int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%d:%d", warehouseID, productID)
	m.inventory[key] += count
	return nil
}

// mockWorldClient is a mock implementation of AmazonWorldClient for testing
type mockWorldClient struct {
	responseChan chan *proto.AResponses
	seqNum       int64
	mu           sync.Mutex
}

func newMockWorldClient() *mockWorldClient {
	return &mockWorldClient{
		responseChan: make(chan *proto.AResponses, 100),
		seqNum:       0,
	}
}

func (m *mockWorldClient) Connect(worldID *int64, warehouses []*proto.AInitWarehouse) (int64, error) {
	// Mock successful connection
	return 1, nil
}

func (m *mockWorldClient) RequestPack(whnum int32, products []*proto.AProduct, shipID int64) (int64, error) {
	m.mu.Lock()
	m.seqNum++
	seqNum := m.seqNum
	m.mu.Unlock()
	return seqNum, nil
}

func (m *mockWorldClient) RequestLoad(whnum int32, truckID int32, shipID int64) (int64, error) {
	m.mu.Lock()
	m.seqNum++
	seqNum := m.seqNum
	m.mu.Unlock()
	return seqNum, nil
}

func (m *mockWorldClient) RequestPurchase(whnum int32, products []*proto.AProduct) (int64, error) {
	m.mu.Lock()
	m.seqNum++
	seqNum := m.seqNum
	m.mu.Unlock()
	return seqNum, nil
}

func (m *mockWorldClient) QueryPackage(packageID int64) (int64, error) {
	m.mu.Lock()
	m.seqNum++
	seqNum := m.seqNum
	m.mu.Unlock()
	return seqNum, nil
}

func (m *mockWorldClient) SendAck(seqNums []int64) error {
	return nil
}

func (m *mockWorldClient) ReceiveResponses() <-chan *proto.AResponses {
	return m.responseChan
}

// mockUpsClient is a mock implementation of UpsServiceClient for testing
type mockUpsClient struct {
	packageIDSeq int64
	truckIDSeq   int32
	mu           sync.Mutex
}

func newMockUpsClient() *mockUpsClient {
	return &mockUpsClient{
		packageIDSeq: 1000,
		truckIDSeq:   1,
	}
}

func (m *mockUpsClient) RequestPickup(ctx context.Context, in *proto.PickupRequest, opts ...grpc.CallOption) (*proto.PickupResp, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.packageIDSeq++
	m.truckIDSeq++

	return &proto.PickupResp{
		PackageId: m.packageIDSeq,
		TruckId:   m.truckIDSeq,
	}, nil
}

func (m *mockUpsClient) RequestRedirect(ctx context.Context, in *proto.Redirect, opts ...grpc.CallOption) (*proto.RedirectResp, error) {
	return &proto.RedirectResp{
		Success: true,
	}, nil
}

func (m *mockUpsClient) RequestCancel(ctx context.Context, in *proto.CancelOrder, opts ...grpc.CallOption) (*proto.CancelResp, error) {
	return &proto.CancelResp{
		Success: true,
	}, nil
}

func (m *mockUpsClient) NotifyLoadReady(ctx context.Context, in *proto.LoadReady, opts ...grpc.CallOption) (*proto.Ack, error) {
	return &proto.Ack{
		Success: true,
		Seqnum:  in.GetSeqnum(),
	}, nil
}

var amazonClient proto.AmazonServiceClient
var testMockDB *mockDatabase

func TestMain(m *testing.M) {
	godotenv.Load("../.env")

	// Create mock database for testing
	testMockDB = newMockDatabase()

	// Create mock world client
	mockWorld := newMockWorldClient()

	// Create mock UPS client
	mockUps := newMockUpsClient()

	// Initialize AmazonServer with mocks
	amazonServer := &AmazonServer{
		database:        testMockDB,
		worldClient:     mockWorld,
		upsClient:       mockUps,
		processedSeqs:   make(map[int64]time.Time),
		retentionPeriod: 5 * time.Minute,
		pendingUpsReqs:  make(map[int64]*pendingUpsRequest),
		seqNum:          0,
	}

	// Pre-create test orders for various tests
	setupTestOrders()

	go func() {
		amazonServer.StartServe()
	}()
	time.Sleep(time.Second)
	amazonClient = amazonclient.NewAmazonClient()
	m.Run()
}

// setupTestOrders creates test orders that will be used across different tests
func setupTestOrders() {
	// Order for TestNotifyRedirectRequest
	packageID1 := int64(5001)
	truckID1 := 10
	testMockDB.orders[9001] = &db.Order{
		OrderID:     9001,
		PackageID:   &packageID1,
		TruckID:     &truckID1,
		UpsUserID:   "testuser",
		DestX:       100,
		DestY:       200,
		WarehouseID: 1,
		Status:      "DELIVERING",
	}
	testMockDB.ordersByPkg[packageID1] = testMockDB.orders[9001]

	// Order for TestNotifyTruckArrived
	packageID2 := int64(6001)
	truckID2 := 15
	testMockDB.orders[9002] = &db.Order{
		OrderID:     9002,
		PackageID:   &packageID2,
		TruckID:     &truckID2,
		UpsUserID:   "testuser2",
		DestX:       150,
		DestY:       250,
		WarehouseID: 2,
		Status:      "PACKED",
	}
	testMockDB.ordersByPkg[packageID2] = testMockDB.orders[9002]

	// Order for TestNotifyDeliveryComplete
	packageID3 := int64(7001)
	truckID3 := 20
	testMockDB.orders[9003] = &db.Order{
		OrderID:     9003,
		PackageID:   &packageID3,
		TruckID:     &truckID3,
		UpsUserID:   "testuser3",
		DestX:       200,
		DestY:       300,
		WarehouseID: 3,
		Status:      "DELIVERING",
	}
	testMockDB.ordersByPkg[packageID3] = testMockDB.orders[9003]

	// Order for TestNotifyDeliveryStarted
	packageID4 := int64(8001)
	truckID4 := 25
	testMockDB.orders[9004] = &db.Order{
		OrderID:     9004,
		PackageID:   &packageID4,
		TruckID:     &truckID4,
		UpsUserID:   "testuser4",
		DestX:       250,
		DestY:       350,
		WarehouseID: 4,
		Status:      "LOADED",
	}
	testMockDB.ordersByPkg[packageID4] = testMockDB.orders[9004]
}

func TestNotifyDeliveryComplete(t *testing.T) {
	ctx := context.Background()

	// Use pre-created test order
	packageID := int64(7001)

	// Use unique sequence number to avoid duplicate message detection
	seqNum := time.Now().UnixNano()

	ack, err := amazonClient.NotifyDeliveryComplete(ctx, &proto.DeliveryComplete{
		Seqnum:    seqNum,
		PackageId: packageID,
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

	// Verify order status was updated to DELIVERED
	testMockDB.mu.RLock()
	updatedOrder := testMockDB.ordersByPkg[packageID]
	if updatedOrder.Status != "DELIVERED" {
		t.Errorf("Expected status DELIVERED, got %s", updatedOrder.Status)
	}
	testMockDB.mu.RUnlock()
}

func TestNotifyDeliveryStarted(t *testing.T) {
	ctx := context.Background()

	// Use pre-created test order
	packageID := int64(8001)

	// Use unique sequence number to avoid duplicate message detection
	seqNum := time.Now().UnixNano()

	ack, err := amazonClient.NotifyDeliveryStarted(ctx, &proto.DeliveryStarted{
		Seqnum:    seqNum,
		PackageId: packageID,
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

	// Verify order status was updated to DELIVERING
	testMockDB.mu.RLock()
	updatedOrder := testMockDB.ordersByPkg[packageID]
	if updatedOrder.Status != "DELIVERING" {
		t.Errorf("Expected status DELIVERING, got %s", updatedOrder.Status)
	}
	testMockDB.mu.RUnlock()
}

func TestNotifyRedirectRequest(t *testing.T) {
	ctx := context.Background()

	// Use pre-created test order
	packageID := int64(5001)
	testOrderID := int64(9001)

	// Use unique sequence number to avoid duplicate message detection
	seqNum := time.Now().UnixNano()

	ack, err := amazonClient.NotifyRedirectRequest(ctx, &proto.Redirect{
		Seqnum:    seqNum,
		PackageId: packageID,
		NewDestination: &proto.Coordinate{
			X: 300,
			Y: 400,
		},
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

	// Verify destination was updated
	testMockDB.mu.RLock()
	updatedOrder := testMockDB.orders[testOrderID]
	if updatedOrder.DestX != 300 || updatedOrder.DestY != 400 {
		t.Errorf("Expected destination (300, 400), got (%d, %d)", updatedOrder.DestX, updatedOrder.DestY)
	}
	testMockDB.mu.RUnlock()
}

func TestNotifyTruckArrived(t *testing.T) {
	ctx := context.Background()

	// Use pre-created test order
	packageID := int64(6001)
	truckID := 15

	// Use unique sequence number to avoid duplicate message detection
	seqNum := time.Now().UnixNano()

	ack, err := amazonClient.NotifyTruckArrived(ctx, &proto.TruckArrived{
		Seqnum:      seqNum,
		PackageId:   packageID,
		TruckId:     int32(truckID),
		WarehouseId: 2,
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

	// Verify order status was updated to LOADING
	testMockDB.mu.RLock()
	updatedOrder := testMockDB.ordersByPkg[packageID]
	if updatedOrder.Status != "LOADING" {
		t.Errorf("Expected status LOADING, got %s", updatedOrder.Status)
	}
	testMockDB.mu.RUnlock()
}

// TestIdempotency tests that duplicate messages are not processed twice
func TestIdempotency(t *testing.T) {
	server := &AmazonServer{
		processedSeqs:   make(map[int64]time.Time),
		retentionPeriod: 5 * time.Minute,
	}

	seqNum := int64(12345)

	// First check - should not be processed
	if server.isProcessed(seqNum) {
		t.Errorf("Expected seqNum %d to not be processed yet", seqNum)
	}

	// Mark as processed
	server.markProcessed(seqNum)

	// Second check - should be processed
	if !server.isProcessed(seqNum) {
		t.Errorf("Expected seqNum %d to be marked as processed", seqNum)
	}
}

// TestSequenceNumberGeneration tests that sequence numbers are unique and increasing
func TestSequenceNumberGeneration(t *testing.T) {
	server := &AmazonServer{
		seqNum: 0,
	}

	seqNums := make(map[int64]bool)
	for i := 0; i < 100; i++ {
		seqNum := server.GetNextSeqNum()
		if seqNums[seqNum] {
			t.Errorf("Duplicate sequence number generated: %d", seqNum)
		}
		seqNums[seqNum] = true

		if seqNum != int64(i+1) {
			t.Errorf("Expected seqNum %d, got %d", i+1, seqNum)
		}
	}
}

// TestPendingRequestTracking tests the pending request mechanism
func TestPendingRequestTracking(t *testing.T) {
	server := &AmazonServer{
		pendingUpsReqs: make(map[int64]*pendingUpsRequest),
	}

	seqNum := int64(999)
	reqType := "loadReady"
	loadReady := &proto.LoadReady{
		Seqnum:    seqNum,
		PackageId: 12345,
	}

	// Add pending request
	server.addPendingUpsRequest(seqNum, reqType, loadReady)

	// Check it exists
	server.pendingMutex.Lock()
	req, exists := server.pendingUpsReqs[seqNum]
	server.pendingMutex.Unlock()

	if !exists {
		t.Errorf("Expected pending request with seqNum %d to exist", seqNum)
	}

	if req.requestType != reqType {
		t.Errorf("Expected request type %s, got %s", reqType, req.requestType)
	}

	// Mark as acknowledged
	server.markUpsRequestAcked(seqNum)

	// Check it's removed
	server.pendingMutex.Lock()
	_, exists = server.pendingUpsReqs[seqNum]
	server.pendingMutex.Unlock()

	if exists {
		t.Errorf("Expected pending request with seqNum %d to be removed after ack", seqNum)
	}
}

// TestCleanupOldSeqNums tests that old sequence numbers are cleaned up
func TestCleanupOldSeqNums(t *testing.T) {
	server := &AmazonServer{
		processedSeqs:   make(map[int64]time.Time),
		retentionPeriod: 100 * time.Millisecond,
	}

	// Add old sequence number
	oldSeqNum := int64(111)
	server.processedSeqs[oldSeqNum] = time.Now().Add(-200 * time.Millisecond)

	// Add recent sequence number
	recentSeqNum := int64(222)
	server.markProcessed(recentSeqNum)

	// Manually trigger cleanup
	server.processedMutex.Lock()
	now := time.Now()
	for seqNum, timestamp := range server.processedSeqs {
		if now.Sub(timestamp) > server.retentionPeriod {
			delete(server.processedSeqs, seqNum)
		}
	}
	server.processedMutex.Unlock()

	// Old one should be removed
	if server.isProcessed(oldSeqNum) {
		t.Errorf("Expected old seqNum %d to be cleaned up", oldSeqNum)
	}

	// Recent one should still exist
	if !server.isProcessed(recentSeqNum) {
		t.Errorf("Expected recent seqNum %d to still exist", recentSeqNum)
	}
}
