package main

import (
	"log/slog"
	"mini-amazon-ups/proto"
	"mini-amazon-ups/ups/server"
	"net"
	"os"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

func main() {
	var err error

	err = godotenv.Load()
	if err != nil {
		panic("Error loading .env file")
	}

	StartUpsServer()
}

func StartUpsServer() {
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
	proto.RegisterUpsServiceServer(grpcServer, server.UpsServer{})
	grpcServer.Serve(lis)
}
