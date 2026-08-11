package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/attike/aristophanes/comedy"
	"github.com/odysseia-greek/ionia/diodoros/bibliotheke"
	v1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"google.golang.org/grpc"
)

const standardPort = ":50060"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = standardPort
	}
	logging.System(`
 ___   ____  ___   ___      ___   ____   ___   _____
|   \ |    |/   \ |   \    /   \ |    \ /   \ / ___/
|    \ |  ||     ||    \  |     ||  _  |     (   \_
|  D  ||  ||  O  ||  D  | |  O  ||  |  |  O  |\__  |
|     ||  ||     ||     | |     ||  |  |     |/  \ |
|     ||  ||     ||     | |     ||  |  |     |\    |
|_____||____|\___/ |_____|  \___/ |__|__|\___/  \___|
`)
	logging.System("Diodoros — βιβλιοθήκη")
	logging.System("starting up and getting env variables")
	cfg, err := bibliotheke.CreateNewConfig(context.Background())
	if err != nil {
		logging.Error(err.Error())
		log.Fatal("death has found me")
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(comedy.UnaryServerInterceptor(cfg.Streamer, comedy.WithHeaderKey(config.HeaderKey), comedy.WithContextKeyName(config.DefaultTracingName), comedy.WithCloseHop())))
	v1.RegisterDiodorosServiceServer(server, cfg)
	logging.Info(fmt.Sprintf("Server listening on %s", port))
	log.Fatal(server.Serve(listener))
}
