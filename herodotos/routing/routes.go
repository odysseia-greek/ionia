package routing

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/odysseia-greek/ionia/herodotos/gateway"
	"github.com/odysseia-greek/ionia/herodotos/graph"
	"github.com/odysseia-greek/ionia/herodotos/middleware"
	"github.com/vektah/gqlparser/v2/ast"
)

func InitRoutes(handlerConfig *gateway.HerodotosHandler) *http.ServeMux {
	mux := http.NewServeMux()
	server := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{Handler: handlerConfig}}))
	server.AddTransport(transport.Options{})
	server.AddTransport(transport.GET{})
	server.AddTransport(transport.POST{})

	server.SetQueryCache(lru.New[*ast.QueryDocument](1000))
	server.Use(extension.Introspection{})

	graphqlHandler := middleware.Adapt(server, middleware.LogRequestDetails(handlerConfig.Streamer))

	mux.Handle("/herodotos/graphql", graphqlHandler)

	mux.HandleFunc("/healthz", writeHealthz)
	mux.HandleFunc("/readyz", writeReadyz)
	return mux
}

func writeHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Healthy bool   `json:"healthy"`
		Time    string `json:"time"`
		Version string `json:"version"`
	}{Healthy: true, Time: time.Now().Format(time.RFC3339), Version: os.Getenv("VERSION")})
}

func writeReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Ready bool `json:"healthy"`
	}{Ready: true})
}
