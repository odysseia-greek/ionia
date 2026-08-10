package main

import (
	"log"
	"net"
	"os"

	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"github.com/odysseia-greek/ionia/thoukydides/polemos"
	"google.golang.org/grpc"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = ":50051"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	forms := &polemos.ElasticFormStore{Address: env("ELASTIC_ADDRESS", "http://localhost:9200"), Index: env("ELASTIC_INDEX", "forms"), Username: os.Getenv("ELASTIC_USERNAME"), Password: os.Getenv("ELASTIC_PASSWORD")}
	v1.RegisterThoukydidesServiceServer(server, polemos.NewService("dev", forms))
	log.Printf("Thoukydides listening on %s", port)
	log.Fatal(server.Serve(listener))
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
