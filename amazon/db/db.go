package db

import (
	"mini-amazon-ups/proto"
	"os"
	"time"

	protobuf "google.golang.org/protobuf/proto"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Database struct {
	db *gorm.DB
}

type Warehouse struct {
	WarehouseID int `gorm:"primaryKey;autoIncrement"`
	X           int `gorm:"not null"`
	Y           int `gorm:"not null"`
}

type Product struct {
	ProductID   int     `gorm:"primaryKey;autoIncrement"`
	Description string  `gorm:"not null"`
	Price       float64 `gorm:"type:decimal(10,2);default:0.00"`
}

type Inventory struct {
	WarehouseID int   `gorm:"primaryKey"`
	ProductID   int64 `gorm:"primaryKey"`
	Count       int   `gorm:"default:0"`
}

// TableName overrides the default table name for Inventory model
func (Inventory) TableName() string {
	return "inventory"
}

type Order struct {
	OrderID     int64  `gorm:"primaryKey;autoIncrement"`
	PackageID   *int64 `gorm:"unique"`
	UpsUserID   string `gorm:"type:varchar(255)"`
	DestX       int    `gorm:"not null"`
	DestY       int    `gorm:"not null"`
	WarehouseID int
	TruckID     *int
	Status      string    `gorm:"type:varchar(50);default:'CREATED'"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

type OrderItem struct {
	OrderID   int64 `gorm:"primaryKey"`
	ProductID int64 `gorm:"primaryKey"`
	Quantity  int   `gorm:"not null"`
}

// NewDatabase initializes the database connection using GORM and returns a Database instance
func NewDatabase() *Database {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		panic("DATABASE_URL environment variable is not set")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	return &Database{db: db}
}

// InitWarehouses initializes the warehouses in the database and returns a list of AInitWarehouse
func (d *Database) InitWarehouses(count int) ([]*proto.AInitWarehouse, error) {
	warehouses := make([]Warehouse, 0, count)
	for i := 1; i <= count; i++ {
		warehouses = append(warehouses, Warehouse{
			X: i * 10,
			Y: i * 10,
		})
	}
	err := d.db.Create(&warehouses).Error
	if err != nil {
		return nil, err
	}

	initWarehouses := make([]*proto.AInitWarehouse, 0, count)
	for _, warehouse := range warehouses {
		initWarehouses = append(initWarehouses, &proto.AInitWarehouse{
			Id: protobuf.Int32(int32(warehouse.WarehouseID)),
			X:  protobuf.Int32(int32(warehouse.X)),
			Y:  protobuf.Int32(int32(warehouse.Y)),
		})
	}
	return initWarehouses, nil
}

// InsertProducts inserts multiple products into the database
func (d *Database) InsertProducts(products ...*Product) error {
	if len(products) == 0 {
		return nil
	}
	return d.db.Create(&products).Error
}

// AddOrUpdateInventory adds inventory for a product in a warehouse, or updates it if it already exists
func (d *Database) AddOrUpdateInventory(warehouseID int, productID int64, count int) error {
	inventory := Inventory{
		WarehouseID: warehouseID,
		ProductID:   productID,
		Count:       count,
	}

	return d.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "warehouse_id"},
			{Name: "product_id"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"count": gorm.Expr("inventory.count + ?", count),
		}),
	}).Create(&inventory).Error
}

func (d *Database) GetInventory(warehouseID int, productID int64) (int, error) {
	var inventory Inventory
	err := d.db.Where("warehouse_id = ? AND product_id = ?", warehouseID, productID).First(&inventory).Error
	if err != nil {
		return 0, err
	}
	return inventory.Count, nil
}

// DecreaseInventory decreases the inventory count for a product in a warehouse
// Returns error if there's not enough inventory
func (d *Database) DecreaseInventory(warehouseID int, productID int64, count int) error {
	return d.db.Transaction(func(tx *gorm.DB) error {
		var inventory Inventory
		err := tx.Where("warehouse_id = ? AND product_id = ?", warehouseID, productID).First(&inventory).Error
		if err != nil {
			return err
		}
		if inventory.Count < count {
			return gorm.ErrRecordNotFound // Not enough inventory
		}
		return tx.Model(&inventory).Update("count", inventory.Count-count).Error
	})
}

// CheckInventoryAvailable checks if there's enough inventory for the given items
func (d *Database) CheckInventoryAvailable(warehouseID int, items map[int64]int) (bool, error) {
	for productID, quantity := range items {
		count, err := d.GetInventory(warehouseID, productID)
		if err != nil {
			return false, err
		}
		if count < quantity {
			return false, nil
		}
	}
	return true, nil
}

// Order operations

// CreateOrder creates a new order with the given details
func (d *Database) CreateOrder(upsUserID string, destX, destY, warehouseID int) (*Order, error) {
	order := &Order{
		UpsUserID:   upsUserID,
		DestX:       destX,
		DestY:       destY,
		WarehouseID: warehouseID,
		Status:      "CREATED",
	}
	err := d.db.Create(order).Error
	if err != nil {
		return nil, err
	}
	return order, nil
}

// GetOrder retrieves an order by its order ID
func (d *Database) GetOrder(orderID int64) (*Order, error) {
	var order Order
	err := d.db.Where("order_id = ?", orderID).First(&order).Error
	if err != nil {
		return nil, err
	}
	return &order, nil
}

// GetOrderByPackageID retrieves an order by its package ID
func (d *Database) GetOrderByPackageID(packageID int64) (*Order, error) {
	var order Order
	err := d.db.Where("package_id = ?", packageID).First(&order).Error
	if err != nil {
		return nil, err
	}
	return &order, nil
}

// UpdateOrderStatus updates the status of an order
func (d *Database) UpdateOrderStatus(orderID int64, status string) error {
	return d.db.Model(&Order{}).Where("order_id = ?", orderID).Update("status", status).Error
}

// UpdateOrderStatusByPackageID updates the status of an order by package ID
func (d *Database) UpdateOrderStatusByPackageID(packageID int64, status string) error {
	return d.db.Model(&Order{}).Where("package_id = ?", packageID).Update("status", status).Error
}

// UpdateOrderTruck updates the truck ID for an order
func (d *Database) UpdateOrderTruck(orderID int64, truckID int) error {
	return d.db.Model(&Order{}).Where("order_id = ?", orderID).Update("truck_id", truckID).Error
}

// UpdateOrderTruckByPackageID updates the truck ID for an order by package ID
func (d *Database) UpdateOrderTruckByPackageID(packageID int64, truckID int) error {
	return d.db.Model(&Order{}).Where("package_id = ?", packageID).Update("truck_id", truckID).Error
}

// UpdateOrderPackageID updates the package ID for an order
func (d *Database) UpdateOrderPackageID(orderID int64, packageID int64) error {
	return d.db.Model(&Order{}).Where("order_id = ?", orderID).Update("package_id", packageID).Error
}

// GetOrdersByStatus retrieves all orders with a given status
func (d *Database) GetOrdersByStatus(status string) ([]Order, error) {
	var orders []Order
	err := d.db.Where("status = ?", status).Find(&orders).Error
	if err != nil {
		return nil, err
	}
	return orders, nil
}

// OrderItem operations

// AddOrderItems adds items to an order
func (d *Database) AddOrderItems(orderID int64, items map[int64]int) error {
	orderItems := make([]OrderItem, 0, len(items))
	for productID, quantity := range items {
		orderItems = append(orderItems, OrderItem{
			OrderID:   orderID,
			ProductID: productID,
			Quantity:  quantity,
		})
	}
	return d.db.Create(&orderItems).Error
}

// GetOrderItems retrieves all items for a given order
func (d *Database) GetOrderItems(orderID int64) ([]OrderItem, error) {
	var items []OrderItem
	err := d.db.Where("order_id = ?", orderID).Find(&items).Error
	if err != nil {
		return nil, err
	}
	return items, nil
}

// GetOrderItemsByPackageID retrieves all items for a given package ID
func (d *Database) GetOrderItemsByPackageID(packageID int64) ([]OrderItem, error) {
	var items []OrderItem
	err := d.db.Raw(`
		SELECT oi.* FROM order_items oi
		JOIN orders o ON oi.order_id = o.order_id
		WHERE o.package_id = ?
	`, packageID).Scan(&items).Error
	if err != nil {
		return nil, err
	}
	return items, nil
}

// CreateOrderWithItems creates an order and its items in a transaction
// Also decreases inventory for the items
func (d *Database) CreateOrderWithItems(upsUserID string, destX, destY, warehouseID int, items map[int64]int) (*Order, error) {
	var order *Order
	err := d.db.Transaction(func(tx *gorm.DB) error {
		// Create order
		order = &Order{
			UpsUserID:   upsUserID,
			DestX:       destX,
			DestY:       destY,
			WarehouseID: warehouseID,
			Status:      "CREATED",
		}
		if err := tx.Create(order).Error; err != nil {
			return err
		}

		// Add order items
		orderItems := make([]OrderItem, 0, len(items))
		for productID, quantity := range items {
			orderItems = append(orderItems, OrderItem{
				OrderID:   order.OrderID,
				ProductID: productID,
				Quantity:  quantity,
			})
		}
		if err := tx.Create(&orderItems).Error; err != nil {
			return err
		}

		// Decrease inventory
		for productID, quantity := range items {
			var inventory Inventory
			if err := tx.Where("warehouse_id = ? AND product_id = ?", warehouseID, productID).First(&inventory).Error; err != nil {
				return err
			}
			if inventory.Count < quantity {
				return gorm.ErrRecordNotFound // Not enough inventory
			}
			if err := tx.Model(&inventory).Update("count", inventory.Count-quantity).Error; err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return order, nil
}
