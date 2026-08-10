package peira

import (
	"context"
	"time"

	gq "github.com/odysseia-greek/ionia/artafernes/internal/graphql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type reading struct {
	ID        string `json:"id"`
	UserID    string `json:"userId"`
	Author    string `json:"author"`
	Book      string `json:"book"`
	Reference string `json:"reference"`
	CreatedAt string `json:"createdAt"`
}

var _ = Describe("guided reading", func() {
	It("starts and retrieves a reading session", func(ctx context.Context) {
		request, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		variables := map[string]any{"input": map[string]any{"userId": "artafernes", "author": "Herodotos", "book": "Histories", "reference": "1.1"}}
		var started struct {
			StartReading reading `json:"startReading"`
		}
		err := gq.Execute(request, baseURL, `mutation($input: StartReadingInput!) { startReading(input: $input) { id userId author book reference createdAt } }`, variables, &started)
		Expect(err).NotTo(HaveOccurred())
		Expect(started.StartReading.ID).NotTo(BeEmpty())

		var retrieved struct {
			Reading reading `json:"reading"`
		}
		err = gq.Execute(request, baseURL, `query($id: ID!) { reading(id: $id) { id userId author book reference createdAt } }`, map[string]any{"id": started.StartReading.ID}, &retrieved)
		Expect(err).NotTo(HaveOccurred())
		Expect(retrieved.Reading).To(Equal(started.StartReading))
	}, SpecTimeout(15*time.Second))
})
