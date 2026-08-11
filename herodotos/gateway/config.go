package gateway

import (
	"context"
	"fmt"
	"os"

	"github.com/odysseia-greek/agora/hesiodos"
	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/attike/aristophanes/comedy"
	arv1 "github.com/odysseia-greek/attike/aristophanes/gen/go/v1"
	"github.com/odysseia-greek/ionia/diodoros/bibliotheke"
	"github.com/odysseia-greek/ionia/thoukydides/polemos"
)

type Config struct {
	Core     *hesiodos.GenericGrpcClient[*polemos.Client]
	Corpus   *hesiodos.GenericGrpcClient[*bibliotheke.Client]
	Streamer arv1.TraceService_ChorusClient
}

func CreateNewConfig(ctx context.Context) (*Config, error) {
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
	_ = os.Getenv(config.EnvVersion)
	return &Config{Core: core, Corpus: corpus, Streamer: streamer}, nil
}
