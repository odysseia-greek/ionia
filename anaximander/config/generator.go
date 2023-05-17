package config

import (
	"github.com/kpango/glg"
	"github.com/odysseia-greek/aristoteles"
	"github.com/odysseia-greek/aristoteles/models"
	"github.com/odysseia-greek/plato/config"
)

const (
	defaultIndex string = "grammar"
)

func CreateNewConfig(env string) (*Config, error) {
	healthCheck := true
	if env == "LOCAL" || env == "TEST" {
		healthCheck = false
	}
	testOverWrite := config.BoolFromEnv(config.EnvTestOverWrite)
	tls := config.BoolFromEnv(config.EnvTlSKey)

	var cfg models.Config

	if healthCheck {
		vaultConfig, err := config.ConfigFromVault()
		if err != nil {
			glg.Error(err)
			return nil, err
		}

		service := aristoteles.ElasticService(tls)

		cfg = models.Config{
			Service:     service,
			Username:    vaultConfig.ElasticUsername,
			Password:    vaultConfig.ElasticPassword,
			ElasticCERT: vaultConfig.ElasticCERT,
		}
	} else {
		cfg = aristoteles.ElasticConfig(env, testOverWrite, tls)
	}

	elastic, err := aristoteles.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	if healthCheck {
		err := aristoteles.HealthCheck(elastic)
		if err != nil {
			return nil, err
		}
	}

	index := config.StringFromEnv(config.EnvIndex, defaultIndex)
	return &Config{
		Index:   index,
		Created: 0,
		Elastic: elastic,
	}, nil
}
