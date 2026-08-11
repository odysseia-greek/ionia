package bibliotheke

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/odysseia-greek/agora/aristoteles"
	elasticmodels "github.com/odysseia-greek/agora/aristoteles/models"
	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/agora/plato/service"
	"github.com/odysseia-greek/attike/aristophanes/comedy"
	arv1 "github.com/odysseia-greek/attike/aristophanes/gen/go/v1"
	"github.com/odysseia-greek/delphi/aristides/diplomat"
	pb "github.com/odysseia-greek/delphi/aristides/proto"
	"google.golang.org/grpc/metadata"
)

func CreateNewConfig(ctx context.Context) (*Service, error) {
	started := time.Now()
	tlsEnabled := config.BoolFromEnv(config.EnvTlSKey)
	version := os.Getenv(config.EnvVersion)
	index := config.StringFromEnv(config.EnvIndex, "text")

	logging.System("Diodoros configuration: connecting to tracing service")
	tracer, err := comedy.NewClientTracer(comedy.DefaultAddress)
	if err != nil {
		return nil, fmt.Errorf("create tracing client: %w", err)
	}
	if !tracer.WaitForHealthyState() {
		return nil, fmt.Errorf("tracing service at %s is not healthy", comedy.DefaultAddress)
	}
	logging.Info(fmt.Sprintf("tracing service is healthy at %s", comedy.DefaultAddress))

	logging.System("Diodoros configuration: opening tracing stream")
	streamer, err := tracer.Chorus(ctx)
	if err != nil {
		return nil, fmt.Errorf("open tracing stream: %w", err)
	}

	logging.System("Diodoros configuration: connecting to ambassador service")
	ambassador, err := diplomat.NewClientAmbassador(diplomat.DEFAULTADDRESS)
	if err != nil {
		return nil, fmt.Errorf("create ambassador client: %w", err)
	}
	if !ambassador.WaitForHealthyState() {
		return nil, fmt.Errorf("ambassador service at %s is not healthy", diplomat.DEFAULTADDRESS)
	}
	logging.Info(fmt.Sprintf("ambassador service is healthy at %s", diplomat.DEFAULTADDRESS))

	traceID := uuid.NewString()
	spanID := comedy.GenerateSpanID()
	combinedID := fmt.Sprintf("%s+%s+%d", traceID, spanID, 1)
	secretCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	secretCtx = metadata.NewOutgoingContext(secretCtx, metadata.Pairs(service.HeaderKey, combinedID))

	traceStart := &arv1.ObserveRequest{
		TraceId: traceID, ParentSpanId: spanID, SpanId: spanID,
		Kind: &arv1.ObserveRequest_TraceStart{TraceStart: &arv1.ObserveTraceStart{
			Method: "GetSecret", Url: diplomat.DEFAULTADDRESS, Operation: "/delphi_ptolemaios.Ptolemaios/GetSecret",
		}},
	}
	if err := streamer.Send(traceStart); err != nil {
		logging.Error(fmt.Sprintf("failed to trace secret request start: %v", err))
	}

	logging.System("Diodoros configuration: retrieving Elasticsearch credentials")
	secret, err := ambassador.GetSecret(secretCtx, &pb.VaultRequest{})
	if err != nil {
		return nil, fmt.Errorf("retrieve Elasticsearch credentials: %w", err)
	}
	traceStop := &arv1.ObserveRequest{
		TraceId: traceID, ParentSpanId: spanID, SpanId: spanID,
		Kind: &arv1.ObserveRequest_TraceStop{TraceStop: &arv1.ObserveTraceStop{
			ResponseBody: fmt.Sprintf("credentials retrieved for Elasticsearch user %s", secret.ElasticUsername),
		}},
	}
	if err := streamer.Send(traceStop); err != nil {
		logging.Error(fmt.Sprintf("failed to trace secret request completion: %v", err))
	}

	elasticAddress := aristoteles.ElasticService(tlsEnabled)
	elasticConfig := elasticmodels.Config{Service: elasticAddress, Username: secret.ElasticUsername, Password: secret.ElasticPassword, ElasticCERT: secret.ElasticCERT}
	logging.System(fmt.Sprintf("Diodoros configuration: creating Elasticsearch client for %s", elasticAddress))
	elastic, err := aristoteles.NewClient(elasticConfig)
	if err != nil {
		return nil, fmt.Errorf("create Elasticsearch client: %w", err)
	}
	logging.System("Diodoros configuration: checking Elasticsearch health")
	if err := aristoteles.HealthCheck(elastic); err != nil {
		return nil, fmt.Errorf("Elasticsearch health check: %w", err)
	}
	logging.Info("Elasticsearch is healthy")

	configured := NewService(&ElasticStore{Client: elastic, Index: index}, version)
	configured.Elastic = elastic
	configured.Index = index
	configured.Streamer = streamer

	logging.System(fmt.Sprintf(`Diodoros Configuration Overview:
- Initialization Time: %s
- Version:             %s
- Tracer Service:      healthy (Address: %s)
- Ambassador Service:  healthy (Address: %s)
- Elasticsearch:       healthy (Address: %s, TLS: %t)
- Elasticsearch Index: %s
`, time.Since(started), version, comedy.DefaultAddress, diplomat.DEFAULTADDRESS, elasticAddress, tlsEnabled, index))
	return configured, nil
}
