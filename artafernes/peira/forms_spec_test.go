package peira

import (
	"context"
	"time"

	gq "github.com/odysseia-greek/ionia/artafernes/internal/graphql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("learning chapters", func() {
	It("lists and serves chapters without answers", func(ctx context.Context) {
		request, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var options struct {
			ChapterOptions struct {
				Chapters []struct {
					Chapter string `json:"chapter"`
				} `json:"chapters"`
			} `json:"chapterOptions"`
		}
		err := gq.Execute(request, baseURL, `query { chapterOptions { chapters { chapter title order level } } }`, nil, &options)
		Expect(err).NotTo(HaveOccurred())
		Expect(options.ChapterOptions.Chapters).NotTo(BeEmpty())

		var response struct {
			Chapter struct {
				Chapter     string `json:"chapter"`
				Description string `json:"description"`
				Context     string `json:"context"`
				Grammar     []struct {
					Grammar string `json:"grammar"`
				} `json:"grammar"`
				Vocabulary []struct {
					Greek string `json:"greek"`
				} `json:"vocabulary"`
				Texts []struct {
					Text         string   `json:"text"`
					ReadingHints []string `json:"readingHints"`
				} `json:"texts"`
			} `json:"chapter"`
		}
		chapter := options.ChapterOptions.Chapters[0].Chapter
		err = gq.Execute(request, baseURL, `query($chapter: String!) { chapter(chapter: $chapter) { chapter description context grammar { grammar } vocabulary { greek } texts { text readingHints } } }`, map[string]any{"chapter": chapter}, &response)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Chapter.Chapter).To(Equal(chapter))
		Expect(response.Chapter.Description).NotTo(BeEmpty())
	}, SpecTimeout(15*time.Second))
})
