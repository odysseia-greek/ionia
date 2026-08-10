package main

import (
	"log"
	"net"
	"os"

	"github.com/odysseia-greek/ionia/diodoros/bibliotheke"
	v1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"google.golang.org/grpc"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = ":50052"
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	v1.RegisterDiodorosServiceServer(server, bibliotheke.NewService(bibliotheke.NewMemoryStore(), "dev"))
	log.Printf("Diodoros listening on %s", port)
	log.Fatal(server.Serve(listener))
}
