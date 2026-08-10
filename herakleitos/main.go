package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/odysseia-greek/agora/plato/logging"
	pb "github.com/odysseia-greek/delphi/aristides/proto"
	"github.com/odysseia-greek/ionia/herakleitos/flux"
)

//go:embed rhema
var rhema embed.FS

func main() {
	//https://patorjk.com/software/taag/#p=display&f=Crawford2&t=HERAKLEITOS
	logging.System(`
 __ __    ___  ____    ____  __  _  _        ___  ____  ______   ___   _____
|  |  |  /  _]|    \  /    ||  |/ ]| |      /  _]|    ||      | /   \ / ___/
|  |  | /  [_ |  D  )|  o  ||  ' / | |     /  [_  |  | |      ||     (   \_ 
|  _  ||    _]|    / |     ||    \ | |___ |    _] |  | |_|  |_||  O  |\__  |
|  |  ||   [_ |    \ |  _  ||     ||     ||   [_  |  |   |  |  |     |/  \ |
|  |  ||     ||  .  \|  |  ||  .  ||     ||     | |  |   |  |  |     |\    |
|__|__||_____||__|\_||__|__||__|\_||_____||_____||____|  |__|   \___/  \___|
                                                                            
`)
	logging.System(strings.Repeat("~", 37))
	logging.System("\"πάντα ῥεῖ\"")
	logging.System("\"everything flows\"")
	logging.System(strings.Repeat("~", 37))

	logging.Debug("creating config")

	handler, err := flux.CreateNewConfig()
	if err != nil {
		logging.Error(err.Error())
		log.Fatal("death has found me")
	}

	root := "rhema"

	err = handler.DeleteIndexAtStartUp()
	if err != nil {
		log.Fatal(err)
	}
	err = handler.CreateIndexAtStartup()
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	documents := 0

	err = fs.WalkDir(rhema, root, func(filePath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			logging.Error(walkErr.Error())
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}

		logging.Debug(fmt.Sprintf("found %s in %s", d.Name(), path.Dir(filePath)))
		plan, err := rhema.ReadFile(filePath)
		if err != nil {
			logging.Error(err.Error())
			return nil
		}

		var rhemai []flux.Text
		err = json.Unmarshal(plan, &rhemai)
		if err != nil {
			logging.Error(fmt.Sprintf("failed to parse %s: %s", filePath, err.Error()))
			return nil
		}
		if len(rhemai) == 0 {
			logging.Debug(fmt.Sprintf("no texts found in %s", filePath))
			return nil
		}

		documents += 1

		wg.Add(1)
		go func(texts []flux.Text) {
			err := handler.Add(texts, &wg)
			if err != nil {
				logging.Error(err.Error())
			}
		}(rhemai)

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()
	logging.Info(fmt.Sprintf("created: %s", strconv.Itoa(handler.Created)))
	logging.Info(fmt.Sprintf("texts found in rhema: %s", strconv.Itoa(documents)))

	logging.Debug("closing Ambassador because job is done")
	// just setting a code that could be used later to check is if it was sent from an actual service
	uuidCode := uuid.New().String()
	_, err = handler.Ambassador.ShutDown(context.Background(), &pb.ShutDownRequest{Code: uuidCode})
	if err != nil {
		logging.Error(err.Error())
	}
	os.Exit(0)
}
