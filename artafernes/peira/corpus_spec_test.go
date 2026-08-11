package peira

import (
	"context"
	gq "github.com/odysseia-greek/ionia/artafernes/internal/graphql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("classical corpus", func() {
	It("serves options and creates a sectioned text", func(ctx context.Context) {
		request, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		var options struct {
			CorpusOptions struct {
				Authors []struct {
					Name string `json:"name"`
				} `json:"authors"`
			} `json:"corpusOptions"`
		}
		err := gq.Execute(request, baseURL, `query { corpusOptions { authors { name } } }`, nil, &options)
		Expect(err).NotTo(HaveOccurred())
		Expect(options.CorpusOptions.Authors).NotTo(BeEmpty())
		var text struct {
			Text struct {
				Author   string `json:"author"`
				Passages []struct {
					Section string `json:"section"`
					Greek   string `json:"greek"`
				} `json:"passages"`
			} `json:"text"`
		}
		variables := map[string]any{"input": map[string]any{"author": "Herodotus", "book": "Histories", "reference": "1.1", "section": "0"}}
		err = gq.Execute(request, baseURL, `query($input: TextInput!) { text(input: $input) { author passages { section greek } } }`, variables, &text)
		Expect(err).NotTo(HaveOccurred())
		Expect(text.Text.Author).To(Equal("Herodotus"))
		Expect(text.Text.Passages).To(HaveLen(1))
	}, SpecTimeout(20*time.Second))
})
