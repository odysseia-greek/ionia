package main

import (
	"github.com/kpango/glg"
	"github.com/odysseia-greek/ionia/melissos/app"
	"github.com/odysseia-greek/ionia/melissos/config"
	"os"
	"strings"
)

func init() {
	errlog := glg.FileWriter("/tmp/error.log", 0666)
	defer errlog.Close()

	glg.Get().
		SetMode(glg.BOTH).
		AddLevelWriter(glg.ERR, errlog)
}

func main() {
	//https://patorjk.com/software/taag/#p=display&f=Crawford2&t=MELISSOS
	glg.Info("\n ___ ___    ___  _      ____ _____ _____  ___   _____\n|   |   |  /  _]| |    |    / ___// ___/ /   \\ / ___/\n| _   _ | /  [_ | |     |  (   \\_(   \\_ |     (   \\_ \n|  \\_/  ||    _]| |___  |  |\\__  |\\__  ||  O  |\\__  |\n|   |   ||   [_ |     | |  |/  \\ |/  \\ ||     |/  \\ |\n|   |   ||     ||     | |  |\\    |\\    ||     |\\    |\n|___|___||_____||_____||____|\\___| \\___| \\___/  \\___|\n                                                     \n")
	glg.Info(strings.Repeat("~", 37))
	glg.Info("\"Οὕτως οὖν ἀίδιόν ἐστι καὶ ἄπειρον καὶ ἓν καὶ ὅμοιον πᾶν.\"")
	glg.Info("\"So then it is eternal and infinite and one and all alike.\"")
	glg.Info(strings.Repeat("~", 37))

	glg.Debug("creating config")

	env := os.Getenv("ENV")

	melissosConfig, err := config.CreateNewConfig(env)
	if err != nil {
		glg.Error(err)
		glg.Fatal("death has found me")
	}

	handler := app.MelissosHandler{
		Config: melissosConfig,
	}

	go handler.PrintProgress()

	handler.Handle()
}
