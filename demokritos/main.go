package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"github.com/kpango/glg"
	"github.com/odysseia-greek/ionia/demokritos/app"
	"github.com/odysseia-greek/ionia/demokritos/config"
	"github.com/odysseia-greek/plato/models"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

var documents int

//go:embed lexiko
var lexiko embed.FS

func init() {
	errlog := glg.FileWriter("/tmp/error.log", 0666)
	defer errlog.Close()

	glg.Get().
		SetMode(glg.BOTH).
		AddLevelWriter(glg.ERR, errlog)
}

func main() {
	//https://patorjk.com/software/taag/#p=display&f=Crawford2&t=DEMOKRITOS
	glg.Info("\n ___      ___  ___ ___   ___   __  _  ____   ____  ______   ___   _____\n|   \\    /  _]|   |   | /   \\ |  |/ ]|    \\ |    ||      | /   \\ / ___/\n|    \\  /  [_ | _   _ ||     ||  ' / |  D  ) |  | |      ||     (   \\_ \n|  D  ||    _]|  \\_/  ||  O  ||    \\ |    /  |  | |_|  |_||  O  |\\__  |\n|     ||   [_ |   |   ||     ||     ||    \\  |  |   |  |  |     |/  \\ |\n|     ||     ||   |   ||     ||  .  ||  .  \\ |  |   |  |  |     |\\    |\n|_____||_____||___|___| \\___/ |__|\\_||__|\\_||____|  |__|   \\___/  \\___|\n                                                                       \n")
	glg.Info(strings.Repeat("~", 37))
	glg.Info("\"νόμωι (γάρ φησι) γλυκὺ καὶ νόμωι πικρόν, νόμωι θερμόν, νόμωι ψυχρόν, νόμωι χροιή, ἐτεῆι δὲ ἄτομα καὶ κενόν\"")
	glg.Info("\"By convention sweet is sweet, bitter is bitter, hot is hot, cold is cold, color is color; but in truth there are only atoms and the void.\"")
	glg.Info(strings.Repeat("~", 37))

	glg.Debug("creating config")

	env := os.Getenv("ENV")

	demokritosConfig, err := config.CreateNewConfig(env)
	if err != nil {
		glg.Error(err)
		glg.Fatal("death has found me")
	}

	root := "lexiko"

	rootDir, err := lexiko.ReadDir(root)
	if err != nil {
		glg.Fatal(err)
	}

	handler := app.DemokritosHandler{Config: demokritosConfig}

	err = handler.DeleteIndexAtStartUp()
	if err != nil {
		glg.Fatal(err)
	}
	err = handler.CreateIndexAtStartup()
	if err != nil {
		glg.Fatal(err)
	}

	var wg sync.WaitGroup

	for _, dir := range rootDir {
		glg.Debug("working on the following directory: " + dir.Name())
		if dir.IsDir() {
			filePath := path.Join(root, dir.Name())
			files, err := lexiko.ReadDir(filePath)
			if err != nil {
				glg.Fatal(err)
			}
			for _, f := range files {
				glg.Debug(fmt.Sprintf("found %s in %s", f.Name(), filePath))
				plan, _ := lexiko.ReadFile(path.Join(filePath, f.Name()))
				var biblos models.Biblos
				err := json.Unmarshal(plan, &biblos)
				if err != nil {
					glg.Fatal(err)
				}

				documents += len(biblos.Biblos)

				wg.Add(1)
				go handler.AddDirectoryToElastic(biblos, &wg)
			}
		}
	}
	go handler.PrintProgress(2 * documents)

	wg.Wait()
	glg.Infof("created: %s", strconv.Itoa(handler.Config.Created))
	glg.Infof("words found in sullego: %s", strconv.Itoa(documents))
	os.Exit(0)
}
