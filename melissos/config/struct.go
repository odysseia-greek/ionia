package config

import (
	elastic "github.com/odysseia-greek/aristoteles"
)

type Config struct {
	Index    string
	Mouseion string
	Created  int
	Elastic  elastic.Client
	Topic    string
	KafkaUrl string
}
