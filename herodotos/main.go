package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/ionia/herodotos/gateway"
	"github.com/odysseia-greek/ionia/herodotos/routing"
)

const standardPort = ":8080"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = standardPort
	} else if port[0] != ':' {
		port = ":" + port
	}
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
	logging.System("Ἡροδότου Ἁλικαρνησσέος ἱστορίης ἀπόδεξις ἥδε")
	logging.System("This is the display of the inquiry of Herodotos of Halikarnassos")
	logging.System("starting up and getting env variables")
	cfg, err := gateway.CreateNewConfig(context.Background())
	if err != nil {
		logging.Error(err.Error())
		log.Fatal("death has found me")
	}
	defer cfg.Close()
	server := routing.InitRoutes(cfg)
	logging.System(fmt.Sprintf("Server running on port %s", port))
	if err := http.ListenAndServe(port, server); err != nil {
		log.Fatal("Server failed to start: ", err)
	}
}
