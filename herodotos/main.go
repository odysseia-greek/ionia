package main

import (
	"log"
	"net/http"
	"os"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	diodorosv1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"github.com/odysseia-greek/ionia/herodotos/graph"
	thoukydidesv1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"github.com/vektah/gqlparser/v2/ast"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	coreConn := dial(env("THOUKYDIDES_ADDRESS", "localhost:50051"))
	defer coreConn.Close()
	corpusConn := dial(env("DIODOROS_ADDRESS", "localhost:50052"))
	defer corpusConn.Close()
	resolver := &graph.Resolver{Core: thoukydidesv1.NewThoukydidesServiceClient(coreConn), Corpus: diodorosv1.NewDiodorosServiceClient(corpusConn)}
	server := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: resolver}))
	server.AddTransport(transport.Options{})
	server.AddTransport(transport.GET{})
	server.AddTransport(transport.POST{})
	server.SetQueryCache(lru.New[*ast.QueryDocument](1000))
	server.Use(extension.Introspection{})
	http.Handle("/", playground.Handler("Herodotos", "/query"))
	http.Handle("/query", server)
	port := env("PORT", "8080")
	log.Printf("Herodotos listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func dial(address string) *grpc.ClientConn {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	return conn
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
