package main

import (
	"context"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/ionia/herodotos/gateway"
	"github.com/odysseia-greek/ionia/herodotos/graph"
	"github.com/vektah/gqlparser/v2/ast"
	"log"
	"net/http"
	"os"
)

func main() {
	logging.System(`
 __ __    ___  ____    ___   ___     ___   ______   ___   _____
|  |  |  /  _]|    \  /   \ |   \   /   \ |      | /   \ / ___/
|  |  | /  [_ |  D  )|     ||    \ |     ||      ||     (   \_
|  _  ||    _]|    / |  O  ||  D  ||  O  ||_|  |_||  O  |\__  |
|  |  ||   [_ |    \ |     ||     ||     |  |  |  |     |/  \ |
|  |  ||     ||  .  \|     ||     ||     |  |  |  |     |\    |
|__|__||_____||__|\_| \___/ |_____| \___/   |__|   \___/  \___|
`)
	logging.System("Herodotos — GraphQL gateway")
	logging.System("starting up and getting env variables")
	cfg, err := gateway.CreateNewConfig(context.Background())
	if err != nil {
		logging.Error(err.Error())
		log.Fatal("death has found me")
	}
	defer cfg.Core.Client.Close()
	defer cfg.Corpus.Client.Close()
	resolver := &graph.Resolver{Core: cfg.Core.Client, Corpus: cfg.Corpus.Client}
	server := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: resolver}))
	server.AddTransport(transport.Options{})
	server.AddTransport(transport.GET{})
	server.AddTransport(transport.POST{})
	server.SetQueryCache(lru.New[*ast.QueryDocument](1000))
	server.Use(extension.Introspection{})
	http.Handle("/", playground.Handler("Herodotos", "/query"))
	http.Handle("/query", server)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	logging.System("Server listening on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
