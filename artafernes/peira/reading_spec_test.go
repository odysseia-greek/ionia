package peira

import (
	"context"
	"time"

	gq "github.com/odysseia-greek/ionia/artafernes/internal/graphql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("checking a chapter", func() {
	It("returns the source, actual and learner text", func(ctx context.Context) {
		request, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		variables := map[string]any{"input": map[string]any{
			"chapter": "chapter-02",
			"answers": []map[string]any{{"text": "john-1-1", "learnerText": "In the beginning was the Word"}},
		}}
		var response struct {
			CheckChapter struct {
				Chapter string `json:"chapter"`
				Texts   []struct {
					Text        string `json:"text"`
					SourceText  string `json:"sourceText"`
					ActualText  string `json:"actualText"`
					LearnerText string `json:"learnerText"`
				} `json:"texts"`
			} `json:"checkChapter"`
		}
		err := gq.Execute(request, baseURL, `mutation($input: CheckChapterInput!) { checkChapter(input: $input) { chapter texts { text sourceText actualText learnerText } } }`, variables, &response)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.CheckChapter.Chapter).To(Equal("chapter-02"))
		Expect(response.CheckChapter.Texts).To(HaveLen(1))
		Expect(response.CheckChapter.Texts[0].ActualText).NotTo(BeEmpty())
		Expect(response.CheckChapter.Texts[0].LearnerText).To(Equal("In the beginning was the Word"))
	}, SpecTimeout(15*time.Second))
})
