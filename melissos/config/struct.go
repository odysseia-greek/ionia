package config

import (
	elastic "github.com/odysseia-greek/aristoteles"
	"github.com/odysseia-greek/eupalinos"
)

type Config struct {
	Index     string
	Created   int
	Updated   int
	Processed int
	Elastic   elastic.Client
	Queue     *eupalinos.Rabbit
}
