package main

import (
	"log/slog"
	"mini-amazon-ups/proto"
	"mini-amazon-ups/ups/server"
	worldups "mini-amazon-ups/world/ups"
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
		panic("Error loading .env file")
	}

	worldClient, err := ConnectToWorld()
	if err != nil {
		panic("Failed to connect to world simulator: " + err.Error())
	}
	StartUpsServer(worldClient)
}

func StartUpsServer(worldClient *worldups.UpsWorldClient) {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "50052" // Default port if not specified
	}
	lis, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		panic("Error starting server")
	}
	slog.Info("[UPS] Server starts to listen on port " + port)
	grpcServer := grpc.NewServer()
	upsServer := &server.UpsServer{
		WorldClient: worldClient,
	}
	proto.RegisterUpsServiceServer(
		grpcServer,
		upsServer,
	)
	grpcServer.Serve(lis)
}

func ConnectToWorld() (*worldups.UpsWorldClient, error) {
	// Connect to world simulator
	worldHost := os.Getenv("WORLD_SERVER_HOST")
	if worldHost == "" {
		worldHost = "localhost"
	}
	worldPort := os.Getenv("WORLD_SERVER_PORT")
	if worldPort == "" {
		worldPort = "12345"
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
	slog.Debug("[UPS] Connecting to world simulator at " + worldHost + ":" + worldPort)
	worldClient, err := worldups.NewUpsWorldClient(worldHost + ":" + worldPort)
	if err != nil {
		return nil, err
	}
	trucks := make([]*proto.UInitTruck, 0, 10)
	for i := range 10 {
		trucks = append(trucks, &proto.UInitTruck{
			Id: protobuf.Int32(int32(i + 1)),
			X:  protobuf.Int32(int32(i * 10)),
			Y:  protobuf.Int32(int32(i * 10)),
		})
	}
	worldID, err := worldClient.Connect(targetWorldID, trucks)
	if err != nil {
		return nil, err
	}
	slog.Info("[UPS] Connected to world simulator with world ID " + strconv.FormatInt(worldID, 10))
	return worldClient, nil
}
