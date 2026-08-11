package peira

import (
	"context"
	"encoding/json"
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
				Chapter string `json:"chapter"`
				Blob    string `json:"blob"`
				Grammar []struct {
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
		err = gq.Execute(request, baseURL, `query($chapter: String!) { chapter(chapter: $chapter) { chapter blob grammar { grammar } vocabulary { greek } texts { text readingHints } } }`, map[string]any{"chapter": chapter}, &response)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Chapter.Chapter).To(Equal(chapter))
		var blob struct {
			ID    string `json:"id"`
			Texts []struct {
				ID          string `json:"id"`
				Translation string `json:"translation"`
			} `json:"texts"`
		}
		Expect(json.Unmarshal([]byte(response.Chapter.Blob), &blob)).To(Succeed())
		Expect(blob.ID).To(Equal(chapter))
		for _, text := range blob.Texts {
			Expect(text.Translation).To(BeEmpty())
		}
	}, SpecTimeout(15*time.Second))
})
