package main

import (
	"context"
	"embed"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/ionia/hekataios/periodos"
	"log"
	"strconv"
	"strings"
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
	if err := handler.Reset(context.Background()); err != nil {
		log.Fatal(err)
	}
	count, err := periodos.Load(context.Background(), data, "rhema", handler)
	if err != nil {
		log.Fatal(err)
	}
	logging.Info("seeded forms: " + strconv.Itoa(count))
}
