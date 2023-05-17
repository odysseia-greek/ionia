package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"github.com/kpango/glg"
	"github.com/odysseia-greek/eupalinos"
	"github.com/odysseia-greek/ionia/parmenides/app"
	"github.com/odysseia-greek/ionia/parmenides/config"
	"github.com/odysseia-greek/plato/models"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

func init() {
	errlog := glg.FileWriter("/tmp/error.log", 0666)
	defer errlog.Close()

	glg.Get().
		SetMode(glg.BOTH).
		AddLevelWriter(glg.ERR, errlog)
}

//go:embed sullego
var sullego embed.FS

func main() {
	//https://patorjk.com/software/taag/#p=display&f=Crawford2&t=PARMENIDES
	glg.Info("\n ____   ____  ____   ___ ___    ___  ____   ____  ___      ___  _____\n|    \\ /    ||    \\ |   |   |  /  _]|    \\ |    ||   \\    /  _]/ ___/\n|  o  )  o  ||  D  )| _   _ | /  [_ |  _  | |  | |    \\  /  [_(   \\_ \n|   _/|     ||    / |  \\_/  ||    _]|  |  | |  | |  D  ||    _]\\__  |\n|  |  |  _  ||    \\ |   |   ||   [_ |  |  | |  | |     ||   [_ /  \\ |\n|  |  |  |  ||  .  \\|   |   ||     ||  |  | |  | |     ||     |\\    |\n|__|  |__|__||__|\\_||___|___||_____||__|__||____||_____||_____| \\___|\n                                                                     \n")
	glg.Info(strings.Repeat("~", 37))
	glg.Info("\"τό γάρ αυτο νοειν έστιν τε καί ειναι\"")
	glg.Info("\"for it is the same thinking and being\"")
	glg.Info(strings.Repeat("~", 37))

	glg.Debug("creating config")

	env := os.Getenv("ENV")

	parmenidesConfig, err := config.CreateNewConfig(env)
	if err != nil {
		glg.Error(err)
		glg.Fatal("death has found me")
	}

	root := "sullego"
	rootDir, err := sullego.ReadDir(root)
	if err != nil {
		glg.Fatal(err)
	}

	handler := app.ParmenidesHandler{Config: parmenidesConfig}

	err = handler.DeleteIndexAtStartUp()
	if err != nil {
		glg.Fatal(err)
	}
	err = handler.CreateIndexAtStartup()
	if err != nil {
		glg.Fatal(err)
	}

	var wg sync.WaitGroup
	documents := 0

	ctx, done := context.WithCancel(context.Background())
	lines := make(chan eupalinos.Message)

	go func() {
		handler.Config.Queue.PubSub().Publish(handler.Config.Queue.PubSub().Redial(ctx), lines)
		done()
	}()

	startUpCode := fmt.Sprintf("startup: %s", handler.Config.ExitCode)
	lines <- eupalinos.Message(startUpCode)

	for _, dir := range rootDir {
		glg.Debug("working on the following directory: " + dir.Name())
		if dir.IsDir() {
			method := dir.Name()
			glg.Infof("working on %s", method)
			methodPath := path.Join(root, dir.Name())
			methodDir, err := sullego.ReadDir(methodPath)
			if err != nil {
				glg.Fatal(err)
			}

			for _, innerDir := range methodDir {
				category := innerDir.Name()
				filePath := path.Join(root, dir.Name(), innerDir.Name())
				files, err := sullego.ReadDir(filePath)
				if err != nil {
					glg.Fatal(err)
				}
				for _, f := range files {
					glg.Debug(fmt.Sprintf("found %s in %s", f.Name(), filePath))
					plan, _ := sullego.ReadFile(path.Join(filePath, f.Name()))
					var logoi models.Logos
					err := json.Unmarshal(plan, &logoi)
					if err != nil {
						glg.Fatal(err)
					}

					documents += len(logoi.Logos)

					wg.Add(1)
					go func() {
						err := handler.Add(logoi, &wg, method, category, lines)
						if err != nil {
							glg.Fatal(err)
						}
					}()
				}
			}
		}

	}
	wg.Wait()
	glg.Infof("created: %s", strconv.Itoa(parmenidesConfig.Created))
	glg.Infof("words found in sullego: %s", strconv.Itoa(documents))

	exitCode := fmt.Sprintf("exitcode: %s", handler.Config.ExitCode)
	lines <- eupalinos.Message(exitCode)

	os.Exit(0)
}
