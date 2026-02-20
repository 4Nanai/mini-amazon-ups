package main

import (
	"log/slog"
	"mini-amazon-ups/amazon/db"
	"mini-amazon-ups/amazon/server"
	"mini-amazon-ups/proto"
	upsclient "mini-amazon-ups/ups/client"
	worldamazon "mini-amazon-ups/world/amazon"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
	protobuf "google.golang.org/protobuf/proto"
)

func init() {
	slog.SetDefault(slog.New(tint.NewHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.Kitchen,
	})))
}

func main() {
	var err error
	err = godotenv.Load()
	if err != nil {
		panic("Failed to load .env file")
	}

	// Init world client & handler
	worldHost := os.Getenv("WORLD_SERVER_HOST")
	if worldHost == "" {
		worldHost = "localhost"
	}
	worldPort := os.Getenv("WORLD_SERVER_PORT")
	if worldPort == "" {
		worldPort = "23456"
	}
	worldClient, err := worldamazon.NewAmazonWorldClient(worldHost + ":" + worldPort)
	if err != nil {
		panic("Failed to create Amazon World client: " + err.Error())
	}
	worldHandler := worldamazon.NewDefaultAmazonWorldHandler()

	// Init ups client
	upsClient := upsclient.NewUpsClient()

	// Init database & warehouses
	database := db.NewDatabase()
	warehouses, err := database.InitWarehouses(5)

	// Start Amazon server
	amazonServer := server.NewAmazonServer(worldClient, upsClient, worldHandler, database)
	worldID, err := amazonServer.ConnectToWorld(nil, warehouses)
	if err != nil {
		panic("Failed to connect to world simulator: " + err.Error())
	}
	slog.Info("Connected to world simulator with world ID " + strconv.FormatInt(worldID, 10))
	amazonServer.StartServe()
}

// Connect to world simulator
func ConnectToWorld() (*worldamazon.AmazonWorldClient, error) {
	worldHost := os.Getenv("WORLD_SERVER_HOST")
	if worldHost == "" {
		worldHost = "localhost"
	}
	worldPort := os.Getenv("WORLD_SERVER_PORT")
	if worldPort == "" {
		worldPort = "23456"
	}
	initWorldID := os.Getenv("WORLD_ID")
	var targetWorldID *int64 = nil
	if initWorldID != "" {
		parsedID, err := strconv.ParseInt(initWorldID, 10, 64)
		if err != nil {
			return nil, err
		}
		targetWorldID = protobuf.Int64(parsedID)
	}
	// Init warehouses
	warehouses := make([]*proto.AInitWarehouse, 0, 10)
	for i := range 10 {
		warehouses = append(warehouses, &proto.AInitWarehouse{
			Id: protobuf.Int32(int32(i + 1)),
			X:  protobuf.Int32(int32(i)),
			Y:  protobuf.Int32(0),
		})
	}

	// slog.Debug("[Amazon] Connecting to world simulator at " + worldHost + ":" + worldPort)
	worldClient, worldID, err := worldamazon.NewAmazonWorldClientAndConnect(worldHost+":"+worldPort, targetWorldID, warehouses)
	if err != nil {
		return nil, err
	}
	slog.Info("[Amazon] Connected to world simulator with world ID " + strconv.FormatInt(worldID, 10))
	return worldClient, nil
}
