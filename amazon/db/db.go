package db

import (
	"mini-amazon-ups/proto"
	"os"

	protobuf "google.golang.org/protobuf/proto"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Database struct {
	db *gorm.DB
}

type Warehouse struct {
	WarehouseID int `gorm:"primaryKey;autoIncrement"`
	X           int `gorm:"not null"`
	Y           int `gorm:"not null"`
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
	d.db.Create(warehouses)
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
