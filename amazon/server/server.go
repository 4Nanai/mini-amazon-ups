package server

import (
	"context"
	"fmt"
	"log/slog"
	"mini-amazon-ups/amazon/db"
	"mini-amazon-ups/proto"
	worldamazon "mini-amazon-ups/world/amazon"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	protobuf "google.golang.org/protobuf/proto"
)

// AmazonServer implements the AmazonService gRPC server
type AmazonServer struct {
	proto.UnimplementedAmazonServiceServer
	worldClient  worldamazon.WorldClientInterface
	upsClient    proto.UpsServiceClient
	worldHandler *worldamazon.AmazonWorldHandler
	database     db.DatabaseInterface

	// Idempotency control - track processed sequence numbers from UPS
	processedSeqs   map[int64]time.Time
	processedMutex  sync.RWMutex
	retentionPeriod time.Duration

	// Pending UPS requests that need acknowledgment
	pendingUpsReqs map[int64]*pendingUpsRequest
	pendingMutex   sync.Mutex

	// Sequence number generation
	seqNum      int64
	seqNumMutex sync.Mutex
}

type pendingUpsRequest struct {
	requestType string // "pickup", "cancel", "redirect", "loadReady"
	data        any
	retryCount  int
	nextRetry   time.Time
	ackReceived bool
}

// NewAmazonServer creates a new AmazonServer instance with the given World client
func NewAmazonServer(worldClient worldamazon.WorldClientInterface, upsClient proto.UpsServiceClient, worldHandler *worldamazon.AmazonWorldHandler, database db.DatabaseInterface) *AmazonServer {
	server := &AmazonServer{
		worldClient:     worldClient,
		upsClient:       upsClient,
		database:        database,
		processedSeqs:   make(map[int64]time.Time),
		retentionPeriod: 5 * time.Minute,
		pendingUpsReqs:  make(map[int64]*pendingUpsRequest),
		seqNum:          0,
	}

	// Create world handler with actual business logic
	server.worldHandler = worldamazon.NewAmazonWorldHandler(
		server.handlePurchaseMore,
		server.handlePacked,
		server.handleLoaded,
		server.handlePackageStatus,
		server.handleWorldError,
	)

	// Start background cleanup and retry loops
	go server.cleanupProcessedSeqsLoop()
	go server.retryUpsRequestsLoop()

	return server
}

func (server *AmazonServer) ConnectToWorld(worldID *int64, warehouses []*proto.AInitWarehouse) (int64, error) {
	if server.worldClient == nil {
		return 0, nil
	}

	var targetWorldID *int64 = nil
	if worldID != nil {
		targetWorldID = worldID
	} else {
		initWorldID := os.Getenv("WORLD_ID")
		if initWorldID != "" {
			parsedID, err := strconv.ParseInt(initWorldID, 10, 64)
			if err != nil {
				return 0, err
			}
			targetWorldID = protobuf.Int64(parsedID)
		}
	}

	worldConnected, err := server.worldClient.Connect(targetWorldID, warehouses)
	return worldConnected, err
}

// GetNextSeqNum generates and returns the next sequence number
func (server *AmazonServer) GetNextSeqNum() int64 {
	server.seqNumMutex.Lock()
	defer server.seqNumMutex.Unlock()
	server.seqNum++
	return server.seqNum
}

// isProcessed checks if a sequence number has already been processed
func (server *AmazonServer) isProcessed(seqNum int64) bool {
	server.processedMutex.RLock()
	defer server.processedMutex.RUnlock()
	_, exists := server.processedSeqs[seqNum]
	return exists
}

// markProcessed marks a sequence number as processed
func (server *AmazonServer) markProcessed(seqNum int64) {
	server.processedMutex.Lock()
	defer server.processedMutex.Unlock()
	server.processedSeqs[seqNum] = time.Now()
}

// cleanupProcessedSeqsLoop periodically removes old processed sequence numbers
func (server *AmazonServer) cleanupProcessedSeqsLoop() {
	ticker := time.NewTicker(server.retentionPeriod)
	defer ticker.Stop()

	for range ticker.C {
		server.processedMutex.Lock()
		now := time.Now()
		for seqNum, timestamp := range server.processedSeqs {
			if now.Sub(timestamp) > server.retentionPeriod {
				delete(server.processedSeqs, seqNum)
			}
		}
		server.processedMutex.Unlock()
	}
}

// addPendingUpsRequest adds a failed UPS request to the retry queue
func (server *AmazonServer) addPendingUpsRequest(seqNum int64, reqType string, data interface{}) {
	server.pendingMutex.Lock()
	defer server.pendingMutex.Unlock()

	// Use exponential backoff starting from 1 second
	initialBackoff := time.Duration(1<<0) * time.Second
	server.pendingUpsReqs[seqNum] = &pendingUpsRequest{
		requestType: reqType,
		data:        data,
		retryCount:  0,
		nextRetry:   time.Now().Add(initialBackoff),
		ackReceived: false,
	}
	slog.Info("[Amazon] Added UPS request to retry queue", "seqNum", seqNum, "type", reqType, "nextRetry", initialBackoff)
}

// markUpsRequestAcked marks a UPS request as acknowledged
func (server *AmazonServer) markUpsRequestAcked(seqNum int64) {
	server.pendingMutex.Lock()
	defer server.pendingMutex.Unlock()

	if req, exists := server.pendingUpsReqs[seqNum]; exists {
		req.ackReceived = true
		delete(server.pendingUpsReqs, seqNum)
		slog.Info("[Amazon] UPS request acknowledged", "seqNum", seqNum, "type", req.requestType)
	}
}

// retryUpsRequestsLoop periodically retries pending UPS requests
func (server *AmazonServer) retryUpsRequestsLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		server.pendingMutex.Lock()
		now := time.Now()

		for seqNum, req := range server.pendingUpsReqs {
			if req.ackReceived {
				delete(server.pendingUpsReqs, seqNum)
				continue
			}

			if now.After(req.nextRetry) {
				req.retryCount++
				if req.retryCount > 4 {
					slog.Error("[Amazon] UPS request failed after max retries",
						"seqNum", seqNum,
						"type", req.requestType,
						"retries", req.retryCount)
					delete(server.pendingUpsReqs, seqNum)
					continue
				}

				backoff := time.Duration(1<<uint(req.retryCount)) * time.Second
				req.nextRetry = now.Add(backoff)

				slog.Warn("[Amazon] Retrying UPS request",
					"seqNum", seqNum,
					"type", req.requestType,
					"retryCount", req.retryCount)

				// Retry based on request type
				go server.retryUpsRequest(seqNum, req)
			}
		}

		server.pendingMutex.Unlock()
	}
}

// retryUpsRequest retries a specific UPS request
func (server *AmazonServer) retryUpsRequest(seqNum int64, req *pendingUpsRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch req.requestType {
	case "loadReady":
		if data, ok := req.data.(*proto.LoadReady); ok {
			_, err := server.upsClient.NotifyLoadReady(ctx, data)
			if err != nil {
				slog.Error("[Amazon] Retry NotifyLoadReady failed", "error", err, "seqNum", seqNum)
			} else {
				slog.Info("[Amazon] Retry NotifyLoadReady succeeded", "seqNum", seqNum)
				server.markUpsRequestAcked(seqNum)
			}
		}
	case "pickup":
		if data, ok := req.data.(*proto.PickupRequest); ok {
			_, err := server.upsClient.RequestPickup(ctx, data)
			if err != nil {
				slog.Error("[Amazon] Retry RequestPickup failed", "error", err, "seqNum", seqNum)
			} else {
				slog.Info("[Amazon] Retry RequestPickup succeeded", "seqNum", seqNum)
				server.markUpsRequestAcked(seqNum)
			}
		}
	case "cancel":
		if data, ok := req.data.(*proto.CancelOrder); ok {
			_, err := server.upsClient.RequestCancel(ctx, data)
			if err != nil {
				slog.Error("[Amazon] Retry RequestCancel failed", "error", err, "seqNum", seqNum)
			} else {
				slog.Info("[Amazon] Retry RequestCancel succeeded", "seqNum", seqNum)
				server.markUpsRequestAcked(seqNum)
			}
		}
	case "redirect":
		if data, ok := req.data.(*proto.Redirect); ok {
			_, err := server.upsClient.RequestRedirect(ctx, data)
			if err != nil {
				slog.Error("[Amazon] Retry RequestRedirect failed", "error", err, "seqNum", seqNum)
			} else {
				slog.Info("[Amazon] Retry RequestRedirect succeeded", "seqNum", seqNum)
				server.markUpsRequestAcked(seqNum)
			}
		}
	}
}

// StartServe starts the Amazon gRPC server and listens for incoming requests
func (server *AmazonServer) StartServe() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "50051"
	}

	lis, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		panic("Failed to listen on port " + port)
	}
	slog.Info("[Amazon] Server starts to listen on port " + port)

	// Register gRPC server and start serving
	grpcServer := grpc.NewServer()
	proto.RegisterAmazonServiceServer(
		grpcServer,
		server,
	)
	grpcServer.Serve(lis)
}

// CreateOrder handles the order creation flow:
// 1. Create order in database
// 2. Request pickup from UPS
// 3. Update order with package_id and truck_id
// 4. Send pack request to World
func (server *AmazonServer) CreateOrder(upsUserID string, destX, destY, warehouseID int, items map[int64]int) (*db.Order, error) {
	// Create order with items and decrease inventory
	order, err := server.database.CreateOrderWithItems(upsUserID, destX, destY, warehouseID, items)
	if err != nil {
		slog.Error("[Amazon] Failed to create order", "error", err)
		return nil, fmt.Errorf("failed to create order: %w", err)
	}

	slog.Info("[Amazon] Order created", "orderID", order.OrderID, "warehouseID", warehouseID)

	// Build items list for UPS
	itemInfos := make([]*proto.ItemInfo, 0, len(items))
	for productID, quantity := range items {
		// Get product description (optional, you may want to query from DB)
		itemInfos = append(itemInfos, &proto.ItemInfo{
			ItemName: fmt.Sprintf("Product-%d", productID),
			Quantity: int32(quantity),
		})
	}

	// Request pickup from UPS with retry support
	seqNum := server.GetNextSeqNum()
	pickupReq := &proto.PickupRequest{
		Seqnum:      seqNum,
		UpsUserId:   upsUserID,
		Items:       itemInfos,
		OrderId:     order.OrderID,
		WarehouseId: int32(warehouseID),
		UserDestination: &proto.Coordinate{
			X: int32(destX),
			Y: int32(destY),
		},
	}

	// Request pickup with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pickupResp, err := server.upsClient.RequestPickup(ctx, pickupReq)
	if err != nil {
		slog.Error("[Amazon] Failed to request pickup from UPS", "error", err, "orderID", order.OrderID)
		// Add to retry queue only on failure
		server.addPendingUpsRequest(seqNum, "pickup", pickupReq)
		return nil, fmt.Errorf("failed to request pickup: %w", err)
	}

	slog.Info("[Amazon] Received pickup response from UPS",
		"orderID", order.OrderID,
		"packageID", pickupResp.GetPackageId(),
		"truckID", pickupResp.GetTruckId())

	// Update order with package_id, truck_id, and status in a single database operation
	packageID := pickupResp.GetPackageId()
	truckID := int(pickupResp.GetTruckId())

	err = server.database.UpdateOrderAfterPickup(order.OrderID, packageID, truckID, "PACKING")
	if err != nil {
		slog.Error("[Amazon] Failed to update order after pickup", "error", err, "orderID", order.OrderID)
		return nil, err
	}

	// Prepare products for World
	products := make([]*proto.AProduct, 0, len(items))
	for productID, quantity := range items {
		products = append(products, &proto.AProduct{
			Id:          protobuf.Int64(productID),
			Description: protobuf.String(fmt.Sprintf("Product-%d", productID)),
			Count:       protobuf.Int32(int32(quantity)),
		})
	}

	// Send pack request to World
	_, err = server.worldClient.RequestPack(int32(warehouseID), products, packageID)
	if err != nil {
		slog.Error("[Amazon] Failed to request pack from World", "error", err, "packageID", packageID)
		return nil, fmt.Errorf("failed to request pack: %w", err)
	}

	slog.Info("[Amazon] Pack request sent to World", "packageID", packageID, "warehouseID", warehouseID)

	// Reload order to get updated data
	order, err = server.database.GetOrder(order.OrderID)
	if err != nil {
		return nil, err
	}

	return order, nil
}

// NotifyTruckArrived handles notifications from UPS service when a truck arrives at a warehouse
func (server *AmazonServer) NotifyTruckArrived(ctx context.Context, req *proto.TruckArrived) (*proto.Ack, error) {
	seqNum := req.GetSeqnum()
	packageID := req.GetPackageId()
	truckID := req.GetTruckId()
	warehouseID := req.GetWarehouseId()

	// Check for duplicate message (idempotency)
	if server.isProcessed(seqNum) {
		slog.Info("[Amazon] Duplicate TruckArrived message, skipping", "seqNum", seqNum, "packageID", packageID)
		return &proto.Ack{
			Success: true,
			Seqnum:  seqNum,
			Message: "Already processed",
		}, nil
	}

	slog.Info("[Amazon] Truck arrived at warehouse",
		"seqNum", seqNum,
		"packageID", packageID,
		"truckID", truckID,
		"warehouseID", warehouseID)

	// Get order by package ID
	order, err := server.database.GetOrderByPackageID(packageID)
	if err != nil {
		slog.Error("[Amazon] Failed to get order by package ID", "error", err, "packageID", packageID)
		return &proto.Ack{
			Success: false,
			Seqnum:  req.Seqnum,
			Message: fmt.Sprintf("Order not found: %v", err),
		}, nil
	}

	// Update order status to LOADING
	err = server.database.UpdateOrderStatusByPackageID(packageID, "LOADING")
	if err != nil {
		slog.Error("[Amazon] Failed to update order status", "error", err, "packageID", packageID)
		return &proto.Ack{
			Success: false,
			Seqnum:  req.Seqnum,
			Message: fmt.Sprintf("Failed to update status: %v", err),
		}, nil
	}

	// Send load request to World (APutOnTruck)
	_, err = server.worldClient.RequestLoad(warehouseID, truckID, packageID)
	if err != nil {
		slog.Error("[Amazon] Failed to request load from World", "error", err, "packageID", packageID)
		return &proto.Ack{
			Success: false,
			Seqnum:  req.Seqnum,
			Message: fmt.Sprintf("Failed to request load: %v", err),
		}, nil
	}

	slog.Info("[Amazon] Load request sent to World",
		"packageID", packageID,
		"truckID", truckID,
		"warehouseID", warehouseID,
		"orderID", order.OrderID)

	// Mark as processed
	server.markProcessed(seqNum)

	return &proto.Ack{
		Success: true,
		Seqnum:  seqNum,
	}, nil
}

// NotifyDeliveryStarted handles notifications from UPS service when a delivery starts
func (server *AmazonServer) NotifyDeliveryStarted(ctx context.Context, req *proto.DeliveryStarted) (*proto.Ack, error) {
	seqNum := req.GetSeqnum()
	packageID := req.GetPackageId()

	// Check for duplicate message (idempotency)
	if server.isProcessed(seqNum) {
		slog.Info("[Amazon] Duplicate DeliveryStarted message, skipping", "seqNum", seqNum, "packageID", packageID)
		return &proto.Ack{
			Success: true,
			Seqnum:  seqNum,
			Message: "Already processed",
		}, nil
	}

	slog.Info("[Amazon] Delivery started", "seqNum", seqNum, "packageID", packageID)

	// Update order status to DELIVERING
	err := server.database.UpdateOrderStatusByPackageID(packageID, "DELIVERING")
	if err != nil {
		slog.Error("[Amazon] Failed to update order status", "error", err, "packageID", packageID)
		return &proto.Ack{
			Success: false,
			Seqnum:  seqNum,
			Message: fmt.Sprintf("Failed to update status: %v", err),
		}, nil
	}

	// Mark as processed
	server.markProcessed(seqNum)

	return &proto.Ack{
		Success: true,
		Seqnum:  seqNum,
	}, nil
}

// NotifyDeliveryComplete handles notifications from UPS service when a delivery is completed
func (server *AmazonServer) NotifyDeliveryComplete(ctx context.Context, req *proto.DeliveryComplete) (*proto.Ack, error) {
	seqNum := req.GetSeqnum()
	packageID := req.GetPackageId()

	// Check for duplicate message (idempotency)
	if server.isProcessed(seqNum) {
		slog.Info("[Amazon] Duplicate DeliveryComplete message, skipping", "seqNum", seqNum, "packageID", packageID)
		return &proto.Ack{
			Success: true,
			Seqnum:  seqNum,
			Message: "Already processed",
		}, nil
	}

	slog.Info("[Amazon] Delivery complete", "seqNum", seqNum, "packageID", packageID)

	// Update order status to DELIVERED
	err := server.database.UpdateOrderStatusByPackageID(packageID, "DELIVERED")
	if err != nil {
		slog.Error("[Amazon] Failed to update order status", "error", err, "packageID", packageID)
		return &proto.Ack{
			Success: false,
			Seqnum:  seqNum,
			Message: fmt.Sprintf("Failed to update status: %v", err),
		}, nil
	}

	// Mark as processed
	server.markProcessed(seqNum)

	return &proto.Ack{
		Success: true,
		Seqnum:  seqNum,
	}, nil
}

// NotifyRedirectRequest handles notifications from UPS service when a redirect request is made
func (server *AmazonServer) NotifyRedirectRequest(ctx context.Context, req *proto.Redirect) (*proto.Ack, error) {
	seqNum := req.GetSeqnum()
	packageID := req.GetPackageId()
	newDest := req.GetNewDestination()

	// Check for duplicate message (idempotency)
	if server.isProcessed(seqNum) {
		slog.Info("[Amazon] Duplicate Redirect message, skipping", "seqNum", seqNum, "packageID", packageID)
		return &proto.Ack{
			Success: true,
			Seqnum:  seqNum,
			Message: "Already processed",
		}, nil
	}

	slog.Info("[Amazon] Redirect request received",
		"seqNum", seqNum,
		"packageID", packageID,
		"newX", newDest.GetX(),
		"newY", newDest.GetY())

	// Get order by package ID to update destination
	order, err := server.database.GetOrderByPackageID(packageID)
	if err != nil {
		slog.Error("[Amazon] Failed to get order by package ID", "error", err, "packageID", packageID)
		return &proto.Ack{
			Success: false,
			Seqnum:  req.Seqnum,
			Message: fmt.Sprintf("Order not found: %v", err),
		}, nil
	}

	// Update destination in database
	err = server.database.UpdateOrderDestination(order.OrderID, int(newDest.GetX()), int(newDest.GetY()))
	if err != nil {
		slog.Error("[Amazon] Failed to update order destination", "error", err, "packageID", packageID)
		return &proto.Ack{
			Success: false,
			Seqnum:  req.Seqnum,
			Message: fmt.Sprintf("Failed to update destination: %v", err),
		}, nil
	}

	slog.Info("[Amazon] Order destination updated",
		"orderID", order.OrderID,
		"packageID", packageID,
		"newX", newDest.GetX(),
		"newY", newDest.GetY())

	// Mark as processed
	server.markProcessed(seqNum)

	return &proto.Ack{
		Success: true,
		Seqnum:  seqNum,
	}, nil
}

// handlePurchaseMore handles APurchaseMore responses from World (goods arrived at warehouse)
func (server *AmazonServer) handlePurchaseMore(seqNum int64, whnum int32, things []*proto.AProduct) {
	slog.Info("[Amazon] Goods arrived at warehouse", "seqNum", seqNum, "warehouseID", whnum, "items", len(things))

	// Update inventory in database
	for _, product := range things {
		productID := product.GetId()
		quantity := int(product.GetCount())

		err := server.database.AddOrUpdateInventory(int(whnum), productID, quantity)
		if err != nil {
			slog.Error("[Amazon] Failed to update inventory",
				"error", err,
				"warehouseID", whnum,
				"productID", productID,
				"quantity", quantity)
			continue
		}

		slog.Info("[Amazon] Inventory updated",
			"warehouseID", whnum,
			"productID", productID,
			"quantity", quantity)
	}
}

// handlePacked handles APacked responses from World (package ready for loading)
func (server *AmazonServer) handlePacked(seqNum int64, shipid int64) {
	slog.Info("[Amazon] Package packed and ready", "seqNum", seqNum, "packageID", shipid)

	// Update order status to PACKED
	err := server.database.UpdateOrderStatusByPackageID(shipid, "PACKED")
	if err != nil {
		slog.Error("[Amazon] Failed to update order status to PACKED", "error", err, "packageID", shipid)
		return
	}

	// Notify UPS that package is ready for loading
	upsSeqNum := server.GetNextSeqNum()
	loadReady := &proto.LoadReady{
		Seqnum:    upsSeqNum,
		PackageId: shipid,
	}

	// Notify UPS with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = server.upsClient.NotifyLoadReady(ctx, loadReady)
	if err != nil {
		slog.Error("[Amazon] Failed to notify UPS load ready", "error", err, "packageID", shipid)
		// Add to retry queue only on failure
		server.addPendingUpsRequest(upsSeqNum, "loadReady", loadReady)
	} else {
		slog.Info("[Amazon] Notified UPS that package is ready", "packageID", shipid)
	}
}

// handleLoaded handles ALoaded responses from World (package loaded on truck)
func (server *AmazonServer) handleLoaded(seqNum int64, shipid int64) {
	slog.Info("[Amazon] Package loaded on truck", "seqNum", seqNum, "packageID", shipid)

	// Update order status to LOADED
	err := server.database.UpdateOrderStatusByPackageID(shipid, "LOADED")
	if err != nil {
		slog.Error("[Amazon] Failed to update order status to LOADED", "error", err, "packageID", shipid)
		return
	}

	slog.Info("[Amazon] Order status updated to LOADED", "packageID", shipid)
}

// handlePackageStatus handles APackage responses from World (package status queries)
func (server *AmazonServer) handlePackageStatus(seqNum int64, packageid int64, status string) {
	slog.Info("[Amazon] Package status received",
		"seqNum", seqNum,
		"packageID", packageid,
		"status", status)

	// Update order status based on World status
	// Map World status to our internal status
	var orderStatus string
	switch status {
	case "packing":
		orderStatus = "PACKING"
	case "packed", "ready":
		orderStatus = "PACKED"
	case "loading":
		orderStatus = "LOADING"
	case "loaded":
		orderStatus = "LOADED"
	case "delivering":
		orderStatus = "DELIVERING"
	case "delivered":
		orderStatus = "DELIVERED"
	default:
		slog.Warn("[Amazon] Unknown package status from World", "status", status, "packageID", packageid)
		return
	}

	err := server.database.UpdateOrderStatusByPackageID(packageid, orderStatus)
	if err != nil {
		slog.Error("[Amazon] Failed to update order status",
			"error", err,
			"packageID", packageid,
			"status", orderStatus)
		return
	}

	slog.Info("[Amazon] Order status updated from World",
		"packageID", packageid,
		"status", orderStatus)

}

// handleWorldError handles AErr responses from World
func (server *AmazonServer) handleWorldError(seqNum int64, originseqnum int64, err string) {
	slog.Error("[Amazon] Error from World simulator",
		"seqNum", seqNum,
		"originSeqNum", originseqnum,
		"error", err)

}

// HandleWorldResponses processes responses from World simulator
// Note: worldClient already handles deduplication, so we don't need to check here
func (server *AmazonServer) HandleWorldResponses() {
	responseChan := server.worldClient.ReceiveResponses()

	for response := range responseChan {
		// Send acknowledgments for received messages
		ackSeqNums := make([]int64, 0)

		// Handle APurchaseMore responses (goods arrived)
		for _, arrived := range response.GetArrived() {
			seqNum := arrived.GetSeqnum()
			server.worldHandler.HandlePurchaseMore(arrived)
			ackSeqNums = append(ackSeqNums, seqNum)
		}

		// Handle APacked responses (package ready for loading)
		for _, packed := range response.GetReady() {
			seqNum := packed.GetSeqnum()
			server.worldHandler.HandlePacked(packed)
			ackSeqNums = append(ackSeqNums, seqNum)
		}

		// Handle ALoaded responses (package loaded on truck)
		for _, loaded := range response.GetLoaded() {
			seqNum := loaded.GetSeqnum()
			server.worldHandler.HandleLoaded(loaded)
			ackSeqNums = append(ackSeqNums, seqNum)
		}

		// Handle APackage responses (package status queries)
		for _, pkg := range response.GetPackagestatus() {
			seqNum := pkg.GetSeqnum()
			server.worldHandler.HandlePackage(pkg)
			ackSeqNums = append(ackSeqNums, seqNum)
		}

		// Handle errors
		for _, errResp := range response.GetError() {
			seqNum := errResp.GetSeqnum()
			server.worldHandler.HandleErr(errResp)
			ackSeqNums = append(ackSeqNums, seqNum)
		}

		// Send acknowledgments
		if len(ackSeqNums) > 0 {
			err := server.worldClient.SendAck(ackSeqNums)
			if err != nil {
				slog.Error("[Amazon] Failed to send acknowledgments to World", "error", err)
			}
		}
	}
}

// CancelOrder requests UPS to cancel a package delivery
func (server *AmazonServer) CancelOrder(packageID int64) error {
	seqNum := server.GetNextSeqNum()
	cancelReq := &proto.CancelOrder{
		Seqnum:    seqNum,
		PackageId: packageID,
	}

	// Request cancel with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := server.upsClient.RequestCancel(ctx, cancelReq)
	if err != nil {
		slog.Error("[Amazon] Failed to request cancel from UPS", "error", err, "packageID", packageID)
		// Add to retry queue only on failure
		server.addPendingUpsRequest(seqNum, "cancel", cancelReq)
		return fmt.Errorf("failed to request cancel: %w", err)
	}

	if !resp.GetSuccess() {
		slog.Warn("[Amazon] Cancel request rejected by UPS", "packageID", packageID, "reason", resp.GetReason())
		return fmt.Errorf("cancel rejected: %s", resp.GetReason())
	}

	// Update order status to CANCELLED
	err = server.database.UpdateOrderStatusByPackageID(packageID, "CANCELLED")
	if err != nil {
		slog.Error("[Amazon] Failed to update order status to CANCELLED", "error", err, "packageID", packageID)
		return err
	}

	slog.Info("[Amazon] Order cancelled successfully", "packageID", packageID)
	return nil
}

// RedirectOrder requests UPS to redirect a package to a new destination
func (server *AmazonServer) RedirectOrder(packageID int64, newX, newY int32) error {
	seqNum := server.GetNextSeqNum()
	redirectReq := &proto.Redirect{
		Seqnum:    seqNum,
		PackageId: packageID,
		NewDestination: &proto.Coordinate{
			X: newX,
			Y: newY,
		},
	}

	// Request redirect with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := server.upsClient.RequestRedirect(ctx, redirectReq)
	if err != nil {
		slog.Error("[Amazon] Failed to request redirect from UPS", "error", err, "packageID", packageID)
		// Add to retry queue only on failure
		server.addPendingUpsRequest(seqNum, "redirect", redirectReq)
		return fmt.Errorf("failed to request redirect: %w", err)
	}

	if !resp.GetSuccess() {
		slog.Warn("[Amazon] Redirect request rejected by UPS", "packageID", packageID, "reason", resp.GetReason())
		return fmt.Errorf("redirect rejected: %s", resp.GetReason())
	}

	// Update destination in database
	order, err := server.database.GetOrderByPackageID(packageID)
	if err != nil {
		slog.Error("[Amazon] Failed to get order by package ID", "error", err, "packageID", packageID)
		return err
	}

	err = server.database.UpdateOrderDestination(order.OrderID, int(newX), int(newY))
	if err != nil {
		slog.Error("[Amazon] Failed to update order destination", "error", err, "packageID", packageID)
		return err
	}

	slog.Info("[Amazon] Order redirected successfully", "packageID", packageID, "newX", newX, "newY", newY)
	return nil
}

// PurchaseInventory requests World to purchase more inventory for a warehouse
func (server *AmazonServer) PurchaseInventory(warehouseID int32, productID int64, quantity int32, description string) error {
	products := []*proto.AProduct{
		{
			Id:          protobuf.Int64(productID),
			Description: protobuf.String(description),
			Count:       protobuf.Int32(quantity),
		},
	}

	_, err := server.worldClient.RequestPurchase(warehouseID, products)
	if err != nil {
		slog.Error("[Amazon] Failed to request purchase from World", "error", err, "warehouseID", warehouseID)
		return fmt.Errorf("failed to request purchase: %w", err)
	}

	slog.Info("[Amazon] Purchase request sent to World",
		"warehouseID", warehouseID,
		"productID", productID,
		"quantity", quantity)

	return nil
}

// QueryPackageStatus queries the status of a package from World
func (server *AmazonServer) QueryPackageStatus(packageID int64) error {
	_, err := server.worldClient.QueryPackage(packageID)
	if err != nil {
		slog.Error("[Amazon] Failed to query package status from World", "error", err, "packageID", packageID)
		return fmt.Errorf("failed to query package: %w", err)
	}

	slog.Info("[Amazon] Package status query sent to World", "packageID", packageID)
	return nil
}
