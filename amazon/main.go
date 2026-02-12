package main

import (
	"log/slog"
	"mini-amazon-ups/amazon/server"
	"mini-amazon-ups/proto"
	"net"
	"os"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

func main() {
	var err error
	err = godotenv.Load()
	if err != nil {
		panic("Failed to load .env file")
	}

	StartAmazonServer()
}

func StartAmazonServer() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "50051" // Default port if not specified
	}

	lis, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		panic("Failed to listen on port " + port)
	}

	slog.Info("[Amazon] Server starts to listen on port " + port)

	grpcServer := grpc.NewServer()
	proto.RegisterAmazonServiceServer(grpcServer, server.AmazonServer{})
	grpcServer.Serve(lis)
}
