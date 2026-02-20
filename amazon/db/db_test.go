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

func TestMain(m *testing.M) {
	godotenv.Load("../.env")
	db = NewDatabase()
	if db.db == nil {
		panic("failed to connect database")
	}
	m.Run()
}
