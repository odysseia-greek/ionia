package peira

import (
	"context"
	"time"

	gq "github.com/odysseia-greek/ionia/artafernes/internal/graphql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type healthResponse struct {
	Health       health `json:"health"`
	CorpusHealth health `json:"corpusHealth"`
}
type health struct {
	Healthy bool   `json:"healthy"`
	Time    string `json:"time"`
	Version string `json:"version"`
}

var _ = Describe("service health", func() {
	It("reports both core and corpus services as healthy", func(ctx context.Context) {
		request, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var response healthResponse
		err := gq.Execute(request, baseURL, `query { health { healthy time version } corpusHealth { healthy time version } }`, nil, &response)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Health.Healthy).To(BeTrue())
		Expect(response.Health.Time).NotTo(BeEmpty())
		Expect(response.CorpusHealth.Healthy).To(BeTrue())
		Expect(response.CorpusHealth.Time).NotTo(BeEmpty())
	}, SpecTimeout(15*time.Second))
})
