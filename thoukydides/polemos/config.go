package polemos

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/odysseia-greek/agora/aristoteles"
	elasticmodels "github.com/odysseia-greek/agora/aristoteles/models"
	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/agora/plato/service"
	"github.com/odysseia-greek/attike/aristophanes/comedy"
	"github.com/odysseia-greek/delphi/aristides/diplomat"
	pb "github.com/odysseia-greek/delphi/aristides/proto"
	"google.golang.org/grpc/metadata"
	"os"
	"time"
)

func CreateNewConfig(ctx context.Context) (*Service, error) {
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
	ambassador, err := diplomat.NewClientAmbassador(diplomat.DEFAULTADDRESS)
	if err != nil {
		return nil, err
	}
	if !ambassador.WaitForHealthyState() {
		return nil, fmt.Errorf("ambassador service is not healthy")
	}
	secretCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	secretCtx = metadata.NewOutgoingContext(secretCtx, metadata.Pairs(service.HeaderKey, uuid.NewString()))
	secret, err := ambassador.GetSecret(secretCtx, &pb.VaultRequest{})
	if err != nil {
		return nil, err
	}
	elastic, err := aristoteles.NewClient(elasticmodels.Config{Service: aristoteles.ElasticService(config.BoolFromEnv(config.EnvTlSKey)), Username: secret.ElasticUsername, Password: secret.ElasticPassword, ElasticCERT: secret.ElasticCERT})
	if err != nil {
		return nil, err
	}
	if err := aristoteles.HealthCheck(elastic); err != nil {
		return nil, err
	}
	index := config.StringFromEnv(config.EnvIndex, "forms")
	configured := NewService(os.Getenv(config.EnvVersion), &ElasticFormStore{Client: elastic, Index: index})
	configured.Elastic = elastic
	configured.Index = index
	configured.Streamer = streamer
	return configured, nil
}
