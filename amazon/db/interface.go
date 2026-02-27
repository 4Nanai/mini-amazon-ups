package db

// DatabaseInterface defines the database operations needed by the server
type DatabaseInterface interface {
	// Order operations
	CreateOrderWithItems(upsUserID string, destX, destY, warehouseID int, items map[int64]int) (*Order, error)
	GetOrder(orderID int64) (*Order, error)
	GetOrderByPackageID(packageID int64) (*Order, error)
	UpdateOrderAfterPickup(orderID int64, packageID int64, truckID int, status string) error
	UpdateOrderStatusByPackageID(packageID int64, status string) error
	UpdateOrderDestination(orderID int64, destX, destY int) error

	// Inventory operations
	AddOrUpdateInventory(warehouseID int, productID int64, count int) error
}

// Ensure *Database implements DatabaseInterface at compile time
var _ DatabaseInterface = (*Database)(nil)
