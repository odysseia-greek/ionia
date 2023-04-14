package config

import (
	elastic "github.com/odysseia-greek/aristoteles"
)

type Config struct {
	Index      string
	SearchWord string
	Created    int
	Elastic    elastic.Client
}
