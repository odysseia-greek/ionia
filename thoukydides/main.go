package main

import (
	"context"
	"fmt"
	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/attike/aristophanes/comedy"
	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"github.com/odysseia-greek/ionia/thoukydides/polemos"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

const standardPort = ":50060"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = standardPort
	}
	logging.System(`
 ______  __ __   ___   __ __  __  _  __ __  ___    ____   ___   _____
|      ||  |  | /   \ |  |  ||  |/ ]|  |  ||   \  |    | /   \ / ___/
|      ||  |  ||     ||  |  ||  ' / |  |  ||    \  |  | |     (   \_
|_|  |_||  _  ||  O  ||  |  ||    \ |  ~  ||  D  | |  | |  O  |\__  |
  |  |  |  |  ||     ||  :  ||     ||___, ||     | |  | |     |/  \ |
  |  |  |  |  ||     ||     ||  .  ||     ||     | |  | |     |\    |
  |__|  |__|__| \___/  \__,_||__|\_||____/ |_____||____| \___/  \___|
`)
	logging.System("Thoukydides — πόλεμος")
	logging.System("starting up and getting env variables")
	cfg, err := polemos.CreateNewConfig(context.Background())
	if err != nil {
		logging.Error(err.Error())
		log.Fatal("death has found me")
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(comedy.UnaryServerInterceptor(cfg.Streamer, comedy.WithHeaderKey(config.HeaderKey), comedy.WithContextKeyName(config.DefaultTracingName), comedy.WithCloseHop())))
	v1.RegisterThoukydidesServiceServer(server, cfg)
	logging.Info(fmt.Sprintf("Server listening on %s", port))
	log.Fatal(server.Serve(listener))
}
