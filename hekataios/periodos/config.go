package periodos

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/odysseia-greek/agora/aristoteles"
	elasticmodels "github.com/odysseia-greek/agora/aristoteles/models"
	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/agora/plato/service"
	"github.com/odysseia-greek/delphi/aristides/diplomat"
	pb "github.com/odysseia-greek/delphi/aristides/proto"
	"google.golang.org/grpc/metadata"
	"time"
)

func CreateNewConfig(ctx context.Context) (*Handler, error) {
	ambassador, err := diplomat.NewClientAmbassador(diplomat.DEFAULTADDRESS)
	if err != nil {
		return nil, err
	}
	if !ambassador.WaitForHealthyState() {
		return nil, fmt.Errorf("ambassador service is not healthy")
	}
	secretCtx, cancel := context.WithTimeout(ctx, time.Minute)
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
	return &Handler{Elastic: elastic, Index: config.StringFromEnv(config.EnvIndex, "forms"), Ambassador: ambassador}, nil
}
