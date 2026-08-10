package peira

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var baseURL string

func TestPeira(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Artafernes System Suite")
}

var _ = BeforeSuite(func() {
	baseURL = os.Getenv("HERODOTOS_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080/query"
	}
})
