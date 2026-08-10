package main

import (
	"context"
	"embed"
	"log"
	"os"

	"github.com/odysseia-greek/ionia/hekataios/periodos"
)

//go:embed rhema
var data embed.FS

func main() {
	sink := &periodos.ElasticSink{Address: env("ELASTIC_ADDRESS", "http://localhost:9200"), Index: env("ELASTIC_INDEX", "forms"), Username: os.Getenv("ELASTIC_USERNAME"), Password: os.Getenv("ELASTIC_PASSWORD")}
	count, err := periodos.Load(context.Background(), data, "rhema", sink)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Hekataios seeded %d forms", count)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
