package db

import (
	"mini-amazon-ups/proto"
	"os"

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

func (d *Database) InsertProducts(products ...*Product) error {
	if len(products) == 0 {
		return nil
	}
	return d.db.Create(&products).Error
}

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
