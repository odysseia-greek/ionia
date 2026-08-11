package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/odysseia-greek/agora/plato/logging"
	pb "github.com/odysseia-greek/delphi/aristides/proto"
	"github.com/odysseia-greek/ionia/hekataios/periodos"
)

//go:embed rhema
var data embed.FS

func main() {
	logging.System(`
 __ __    ___  __  _   ____  ______   ____  ____   ___   _____
|  |  |  /  _]|  |/ ] /    ||      | /    ||    | /   \ / ___/
|  |  | /  [_ |  ' / |  o  ||      ||  o  | |  ||     (   \_
|  _  ||    _]|    \ |     ||_|  |_||     | |  ||  O  |\__  |
|  |  ||   [_ |     ||  _  |  |  |  |  _  | |  ||     |/  \ |
|  |  ||     ||  .  ||  |  |  |  |  |  |  | |  ||     |\    |
|__|__||_____||__|\_||__|__|  |__|  |__|__||____| \___/  \___|
`)
	logging.System(strings.Repeat("~", 37))
	logging.System("Hekataios — περίοδος")
	logging.System(strings.Repeat("~", 37))
	logging.Debug("creating config")
	handler, err := periodos.CreateNewConfig(context.Background())
	if err != nil {
		logging.Error(err.Error())
		log.Fatal("death has found me")
	}
	logging.Debug("deleting forms index before seeding")
	if err := handler.DeleteIndexAtStartUp(context.Background()); err != nil {
		log.Fatal(err)
	}
	logging.Debug("creating forms index")
	if err := handler.CreateIndexAtStartup(context.Background()); err != nil {
		log.Fatal(err)
	}

	logging.Debug("loading embedded forms")
	count, err := periodos.Load(context.Background(), data, "rhema", handler)
	if err != nil {
		log.Fatal(err)
	}
	logging.Info("seeded forms: " + strconv.Itoa(count))

	logging.Debug("closing Ambassador because the seed job is done")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownCode := uuid.NewString()
	if _, err := handler.Ambassador.ShutDown(shutdownCtx, &pb.ShutDownRequest{Code: shutdownCode}); err != nil {
		logging.Error(fmt.Sprintf("failed to shut down Ambassador: %v", err))
		return
	}
	logging.Info("Ambassador shut down successfully")
}
