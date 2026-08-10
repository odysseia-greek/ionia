package peira

import (
	"context"
	"encoding/json"
	"time"

	gq "github.com/odysseia-greek/ionia/artafernes/internal/graphql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("learning forms", func() {
	It("serves Elasticsearch form blobs through the gateway", func(ctx context.Context) {
		request, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var response struct {
			Forms []struct {
				ID   string `json:"id"`
				Blob string `json:"blob"`
			} `json:"forms"`
		}
		err := gq.Execute(request, baseURL, `query { forms(size: 1) { id blob } }`, nil, &response)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Forms).NotTo(BeEmpty())
		var blob struct {
			ID      string            `json:"id"`
			Grammar []json.RawMessage `json:"grammar"`
			Texts   []json.RawMessage `json:"texts"`
		}
		Expect(json.Unmarshal([]byte(response.Forms[0].Blob), &blob)).To(Succeed())
		Expect(blob.ID).To(Equal(response.Forms[0].ID))
		Expect(blob.Texts).NotTo(BeEmpty())
	}, SpecTimeout(15*time.Second))
})
