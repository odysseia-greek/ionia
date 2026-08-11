package peira

import (
	"context"
	"time"

	gq "github.com/odysseia-greek/ionia/artafernes/internal/graphql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type reading struct {
	ID           string `json:"id"`
	UserID       string `json:"userId"`
	FormID       string `json:"formId"`
	ProgressBlob string `json:"progressBlob"`
	CreatedAt    string `json:"createdAt"`
	UpdatedAt    string `json:"updatedAt"`
}

var _ = Describe("guided reading", func() {
	It("starts and retrieves a reading session", func(ctx context.Context) {
		request, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		variables := map[string]any{"input": map[string]any{"userId": "artafernes", "formId": "chapter-02"}}
		var started struct {
			StartReading reading `json:"startReading"`
		}
		err := gq.Execute(request, baseURL, `mutation($input: StartReadingInput!) { startReading(input: $input) { id userId formId progressBlob createdAt updatedAt } }`, variables, &started)
		Expect(err).NotTo(HaveOccurred())
		Expect(started.StartReading.ID).NotTo(BeEmpty())

		var saved struct {
			SaveProgress reading `json:"saveProgress"`
		}
		err = gq.Execute(request, baseURL, `mutation($input: SaveProgressInput!) { saveProgress(input: $input) { id userId formId progressBlob createdAt updatedAt } }`, map[string]any{"input": map[string]any{"id": started.StartReading.ID, "progressBlob": `{"step":1}`}}, &saved)
		Expect(err).NotTo(HaveOccurred())
		Expect(saved.SaveProgress.ProgressBlob).To(Equal(`{"step":1}`))

		var retrieved struct {
			Reading reading `json:"reading"`
		}
		err = gq.Execute(request, baseURL, `query($id: ID!) { reading(id: $id) { id userId formId progressBlob createdAt updatedAt } }`, map[string]any{"id": started.StartReading.ID}, &retrieved)
		Expect(err).NotTo(HaveOccurred())
		Expect(retrieved.Reading).To(Equal(saved.SaveProgress))
	}, SpecTimeout(15*time.Second))
})
