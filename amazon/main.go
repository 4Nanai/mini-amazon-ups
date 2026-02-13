package main

import (
	"log/slog"
	"mini-amazon-ups/amazon/server"
	"mini-amazon-ups/proto"
	worldamazon "mini-amazon-ups/world/amazon"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
	"google.golang.org/grpc"
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

	worldClient, err := ConnectToWorld()
	if err != nil {
		panic("Failed to connect to world simulator: " + err.Error())
	}
	StartAmazonServer(worldClient)
}

func StartAmazonServer(worldClient *worldamazon.AmazonWorldClient) {
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
		server.AmazonServer{
			WorldClient: worldClient,
		})
	grpcServer.Serve(lis)
}

func ConnectToWorld() (*worldamazon.AmazonWorldClient, error) {
	// Connect to world simulator
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
	// slog.Debug("[Amazon] Connecting to world simulator at " + worldHost + ":" + worldPort)
	worldClient, err := worldamazon.NewAmazonWorldClient(worldHost + ":" + worldPort)
	if err != nil {
		return nil, err
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

	worldID, err := worldClient.Connect(targetWorldID, warehouses)
	if err != nil {
		return nil, err
	}
	slog.Info("[Amazon] Connected to world simulator with world ID " + strconv.FormatInt(worldID, 10))
	return worldClient, nil
}
