package db

import (
	"log/slog"
	"testing"

	"github.com/joho/godotenv"
)

var db *Database

func TestAddOrUpdateInventory(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(2)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}
	products := []*Product{
		{
			Description: "Product in wh 1",
			Price:       114.514,
		},
		{
			Description: "Product in wh 2",
			Price:       1919.810,
		},
	}
	err = testdb.InsertProducts(products...)
	if err != nil {
		t.Fatalf("failed to insert products: %v", err)
	}

	err = testdb.AddOrUpdateInventory(1, int64(products[0].ProductID), 3)
	if err != nil {
		t.Fatalf("failed to add inventory: %v", err)
	}
	err = testdb.AddOrUpdateInventory(2, int64(products[1].ProductID), 2)
	if err != nil {
		t.Fatalf("failed to add inventory: %v", err)
	}
	count, err := testdb.GetInventory(1, int64(products[0].ProductID))
	if count != 3 {
		t.Fatalf("expected inventory count to be 3, got %d", count)
	}
	count, err = testdb.GetInventory(2, int64(products[1].ProductID))
	if count != 2 {
		t.Fatalf("expected inventory count to be 2, got %d", count)
	}

	// Update inventory
	err = testdb.AddOrUpdateInventory(1, int64(products[0].ProductID), 2)
	if err != nil {
		t.Fatalf("failed to update inventory: %v", err)
	}
	count, err = testdb.GetInventory(1, int64(products[0].ProductID))
	if count != 5 {
		t.Fatalf("expected inventory count to be 5 after update, got %d", count)
	}
	slog.Info("inventory added and updated successfully")
}

func TestInitWarehouses(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	warehouses, err := testdb.InitWarehouses(5)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}
	if len(warehouses) != 5 {
		t.Fatalf("expected 5 warehouses, got %d", len(warehouses))
	}
	for _, warehouse := range warehouses {
		if *warehouse.Id == 0 {
			t.Fatalf("expected warehouse ID to be set, got 0")
		}
	}
	slog.Info("warehouses initialized successfully", "warehouses", warehouses)
}

func TestInsertProducts(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	products := []*Product{
		{
			Description: "Product 1",
			Price:       114.514,
		},
		{
			Description: "Product 2",
			Price:       1919.810,
		},
	}

	err := testdb.InsertProducts(products...)
	if err != nil {
		t.Fatalf("failed to insert products: %v", err)
	}
	for _, product := range products {
		if product.ProductID == 0 {
			t.Fatalf("expected product ID to be set, got 0")
		}
	}
	slog.Info("products inserted successfully", "products", products)
}

func TestDecreaseInventory(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	products := []*Product{
		{Description: "Test Product", Price: 100.0},
	}
	err = testdb.InsertProducts(products...)
	if err != nil {
		t.Fatalf("failed to insert products: %v", err)
	}

	// Add inventory
	err = testdb.AddOrUpdateInventory(1, int64(products[0].ProductID), 10)
	if err != nil {
		t.Fatalf("failed to add inventory: %v", err)
	}

	// Decrease inventory
	err = testdb.DecreaseInventory(1, int64(products[0].ProductID), 3)
	if err != nil {
		t.Fatalf("failed to decrease inventory: %v", err)
	}

	count, err := testdb.GetInventory(1, int64(products[0].ProductID))
	if err != nil {
		t.Fatalf("failed to get inventory: %v", err)
	}
	if count != 7 {
		t.Fatalf("expected inventory count to be 7, got %d", count)
	}

	// Try to decrease more than available
	err = testdb.DecreaseInventory(1, int64(products[0].ProductID), 10)
	if err == nil {
		t.Fatalf("expected error when decreasing more than available inventory")
	}

	slog.Info("inventory decreased successfully")
}

func TestCheckInventoryAvailable(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	products := []*Product{
		{Description: "Product 1", Price: 100.0},
		{Description: "Product 2", Price: 200.0},
	}
	err = testdb.InsertProducts(products...)
	if err != nil {
		t.Fatalf("failed to insert products: %v", err)
	}

	err = testdb.AddOrUpdateInventory(1, int64(products[0].ProductID), 10)
	if err != nil {
		t.Fatalf("failed to add inventory: %v", err)
	}
	err = testdb.AddOrUpdateInventory(1, int64(products[1].ProductID), 5)
	if err != nil {
		t.Fatalf("failed to add inventory: %v", err)
	}

	items := map[int64]int{
		int64(products[0].ProductID): 5,
		int64(products[1].ProductID): 3,
	}
	available, err := testdb.CheckInventoryAvailable(1, items)
	if err != nil {
		t.Fatalf("failed to check inventory: %v", err)
	}
	if !available {
		t.Fatalf("expected inventory to be available")
	}

	items[int64(products[1].ProductID)] = 10
	available, err = testdb.CheckInventoryAvailable(1, items)
	if err != nil {
		t.Fatalf("failed to check inventory: %v", err)
	}
	if available {
		t.Fatalf("expected inventory to be unavailable")
	}

	slog.Info("inventory check passed")
}

func TestCreateOrder(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	order, err := testdb.CreateOrder("user123", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	if order.OrderID == 0 {
		t.Fatalf("expected order ID to be set")
	}
	if order.Status != "CREATED" {
		t.Fatalf("expected order status to be CREATED, got %s", order.Status)
	}
	if order.UpsUserID != "user123" {
		t.Fatalf("expected ups user ID to be user123, got %s", order.UpsUserID)
	}

	slog.Info("order created successfully", "order", order)
}

func TestGetOrder(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	order, err := testdb.CreateOrder("user123", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	retrieved, err := testdb.GetOrder(order.OrderID)
	if err != nil {
		t.Fatalf("failed to get order: %v", err)
	}

	if retrieved.OrderID != order.OrderID {
		t.Fatalf("expected order ID to be %d, got %d", order.OrderID, retrieved.OrderID)
	}

	slog.Info("order retrieved successfully", "order", retrieved)
}

func TestUpdateOrderStatus(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	order, err := testdb.CreateOrder("user123", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	err = testdb.UpdateOrderStatus(order.OrderID, "PACKING")
	if err != nil {
		t.Fatalf("failed to update order status: %v", err)
	}

	updated, err := testdb.GetOrder(order.OrderID)
	if err != nil {
		t.Fatalf("failed to get updated order: %v", err)
	}

	if updated.Status != "PACKING" {
		t.Fatalf("expected order status to be PACKING, got %s", updated.Status)
	}

	slog.Info("order status updated successfully", "order", updated)
}

func TestUpdateOrderPackageID(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	order, err := testdb.CreateOrder("user123", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	packageID := int64(12345)
	err = testdb.UpdateOrderPackageID(order.OrderID, packageID)
	if err != nil {
		t.Fatalf("failed to update order package ID: %v", err)
	}

	updated, err := testdb.GetOrder(order.OrderID)
	if err != nil {
		t.Fatalf("failed to get updated order: %v", err)
	}

	if updated.PackageID == nil || *updated.PackageID != packageID {
		t.Fatalf("expected package ID to be %d, got %v", packageID, updated.PackageID)
	}

	// Test GetOrderByPackageID
	retrieved, err := testdb.GetOrderByPackageID(packageID)
	if err != nil {
		t.Fatalf("failed to get order by package ID: %v", err)
	}

	if retrieved.OrderID != order.OrderID {
		t.Fatalf("expected order ID to be %d, got %d", order.OrderID, retrieved.OrderID)
	}

	slog.Info("order package ID updated successfully", "order", updated)
}

func TestUpdateOrderDestination(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	order, err := testdb.CreateOrder("user123", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	newX := 300
	newY := 400
	err = testdb.UpdateOrderDestination(order.OrderID, newX, newY)
	if err != nil {
		t.Fatalf("failed to update order destination: %v", err)
	}

	updated, err := testdb.GetOrder(order.OrderID)
	if err != nil {
		t.Fatalf("failed to get updated order: %v", err)
	}

	if updated.DestX != newX || updated.DestY != newY {
		t.Fatalf("expected destination to be (%d, %d), got (%d, %d)", newX, newY, updated.DestX, updated.DestY)
	}

	slog.Info("order destination updated successfully", "order", updated)
}

func TestUpdateOrderTruck(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	order, err := testdb.CreateOrder("user123", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	truckID := 42
	err = testdb.UpdateOrderTruck(order.OrderID, truckID)
	if err != nil {
		t.Fatalf("failed to update order truck ID: %v", err)
	}

	updated, err := testdb.GetOrder(order.OrderID)
	if err != nil {
		t.Fatalf("failed to get updated order: %v", err)
	}

	if updated.TruckID == nil || *updated.TruckID != truckID {
		t.Fatalf("expected truck ID to be %d, got %v", truckID, updated.TruckID)
	}

	slog.Info("order truck ID updated successfully", "order", updated)
}

func TestGetOrdersByStatus(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	_, err = testdb.CreateOrder("user1", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	order2, err := testdb.CreateOrder("user2", 150, 250, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	err = testdb.UpdateOrderStatus(order2.OrderID, "PACKING")
	if err != nil {
		t.Fatalf("failed to update order status: %v", err)
	}

	createdOrders, err := testdb.GetOrdersByStatus("CREATED")
	if err != nil {
		t.Fatalf("failed to get orders by status: %v", err)
	}

	if len(createdOrders) != 1 {
		t.Fatalf("expected 1 order with CREATED status, got %d", len(createdOrders))
	}

	packingOrders, err := testdb.GetOrdersByStatus("PACKING")
	if err != nil {
		t.Fatalf("failed to get orders by status: %v", err)
	}

	if len(packingOrders) != 1 {
		t.Fatalf("expected 1 order with PACKING status, got %d", len(packingOrders))
	}

	slog.Info("orders retrieved by status successfully")
}

func TestAddOrderItems(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	products := []*Product{
		{Description: "Product 1", Price: 100.0},
		{Description: "Product 2", Price: 200.0},
	}
	err = testdb.InsertProducts(products...)
	if err != nil {
		t.Fatalf("failed to insert products: %v", err)
	}

	order, err := testdb.CreateOrder("user123", 100, 200, 1)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	items := map[int64]int{
		int64(products[0].ProductID): 3,
		int64(products[1].ProductID): 5,
	}
	err = testdb.AddOrderItems(order.OrderID, items)
	if err != nil {
		t.Fatalf("failed to add order items: %v", err)
	}

	retrievedItems, err := testdb.GetOrderItems(order.OrderID)
	if err != nil {
		t.Fatalf("failed to get order items: %v", err)
	}

	if len(retrievedItems) != 2 {
		t.Fatalf("expected 2 order items, got %d", len(retrievedItems))
	}

	slog.Info("order items added successfully", "items", retrievedItems)
}

func TestCreateOrderWithItems(t *testing.T) {
	tx := db.db.Begin()
	testdb := &Database{db: tx}
	defer tx.Rollback()

	_, err := testdb.InitWarehouses(1)
	if err != nil {
		t.Fatalf("failed to initialize warehouses: %v", err)
	}

	products := []*Product{
		{Description: "Product 1", Price: 100.0},
		{Description: "Product 2", Price: 200.0},
	}
	err = testdb.InsertProducts(products...)
	if err != nil {
		t.Fatalf("failed to insert products: %v", err)
	}

	// Add inventory
	err = testdb.AddOrUpdateInventory(1, int64(products[0].ProductID), 10)
	if err != nil {
		t.Fatalf("failed to add inventory: %v", err)
	}
	err = testdb.AddOrUpdateInventory(1, int64(products[1].ProductID), 10)
	if err != nil {
		t.Fatalf("failed to add inventory: %v", err)
	}

	items := map[int64]int{
		int64(products[0].ProductID): 3,
		int64(products[1].ProductID): 5,
	}

	order, err := testdb.CreateOrderWithItems("user123", 100, 200, 1, items)
	if err != nil {
		t.Fatalf("failed to create order with items: %v", err)
	}

	if order.OrderID == 0 {
		t.Fatalf("expected order ID to be set")
	}

	// Check order items
	retrievedItems, err := testdb.GetOrderItems(order.OrderID)
	if err != nil {
		t.Fatalf("failed to get order items: %v", err)
	}

	if len(retrievedItems) != 2 {
		t.Fatalf("expected 2 order items, got %d", len(retrievedItems))
	}

	// Check inventory was decreased
	count1, err := testdb.GetInventory(1, int64(products[0].ProductID))
	if err != nil {
		t.Fatalf("failed to get inventory: %v", err)
	}
	if count1 != 7 {
		t.Fatalf("expected inventory count to be 7, got %d", count1)
	}

	count2, err := testdb.GetInventory(1, int64(products[1].ProductID))
	if err != nil {
		t.Fatalf("failed to get inventory: %v", err)
	}
	if count2 != 5 {
		t.Fatalf("expected inventory count to be 5, got %d", count2)
	}

	slog.Info("order with items created successfully", "order", order)
}

func TestMain(m *testing.M) {
	godotenv.Load("../.env")
	db = NewDatabase()
	if db.db == nil {
		panic("failed to connect database")
	}
	m.Run()
}
