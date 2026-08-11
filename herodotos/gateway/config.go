package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/odysseia-greek/agora/hesiodos"
	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/attike/aristophanes/comedy"
	"github.com/odysseia-greek/ionia/diodoros/bibliotheke"
	"github.com/odysseia-greek/ionia/thoukydides/polemos"
)

func CreateNewConfig(ctx context.Context) (*HerodotosHandler, error) {
	start := time.Now()
	tracer, err := comedy.NewClientTracer(comedy.DefaultAddress)
	if err != nil {
		return nil, err
	}
	if !tracer.WaitForHealthyState() {
		return nil, fmt.Errorf("tracing service is not healthy")
	}
	streamer, err := tracer.Chorus(ctx)
	if err != nil {
		return nil, err
	}
	coreAddress := config.StringFromEnv("THOUKYDIDES_ADDRESS", "thoukydides:50060")
	core, err := hesiodos.NewGenericGrpcClient[*polemos.Client](coreAddress, polemos.NewClient)
	if err != nil {
		return nil, err
	}
	corpusAddress := config.StringFromEnv("DIODOROS_ADDRESS", "diodoros:50060")
	corpus, err := hesiodos.NewGenericGrpcClient[*bibliotheke.Client](corpusAddress, bibliotheke.NewClient)
	if err != nil {
		_ = core.Client.Close()
		return nil, err
	}

	coreHealthy := core.Client.WaitForHealthyState()
	corpusHealthy := corpus.Client.WaitForHealthyState()

	logging.System(fmt.Sprintf(`Herodotos Configuration Overview:
- Initialization Time: %s
- Tracer Service:      %v (Address: %s)
- Thoukydides Service: %v (Address: %s)
- Diodoros Service:    %v (Address: %s)
`, time.Since(start), true, comedy.DefaultAddress, coreHealthy, coreAddress, corpusHealthy, corpusAddress))
	return &HerodotosHandler{Core: core, Corpus: corpus, Streamer: streamer}, nil
}
