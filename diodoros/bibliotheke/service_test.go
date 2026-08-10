package bibliotheke

import (
	"context"
	"testing"

	v1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

func fixture() *v1.Text {
	return &v1.Text{Author: "Herodotos", Book: "Histories", Reference: "1.1", Passages: []*v1.Passage{{Section: "a", Greek: "Ἡροδότου", Translations: []string{"This is a sentence"}}, {Section: "b", Greek: "Ἁλικαρνησσέος"}}}
}

func TestCreateTextSelectsSection(t *testing.T) {
	service := NewService(NewMemoryStore(fixture()), "test")
	text, err := service.CreateText(context.Background(), &v1.CreateTextRequest{Author: "herodotos", Book: "histories", Reference: "1.1", Section: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(text.Passages) != 1 || text.Passages[0].Section != "a" {
		t.Fatalf("unexpected passages: %#v", text.Passages)
	}
}

func TestOptionsBuildsCorpusHierarchy(t *testing.T) {
	service := NewService(NewMemoryStore(fixture()), "test")
	options, err := service.Options(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if len(options.Authors) != 1 || len(options.Authors[0].Books[0].References[0].Sections) != 2 {
		t.Fatalf("unexpected options: %#v", options)
	}
}

func TestCheckTextPreservesLegacyScoring(t *testing.T) {
	service := NewService(NewMemoryStore(fixture()), "test")
	result, err := service.CheckText(context.Background(), &v1.CheckTextRequest{Author: "Herodotos", Book: "Histories", Reference: "1.1", Translations: []*v1.TranslationAnswer{{Section: "a", Translation: "This is a sentence"}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.AverageLevenshteinPercentage != "100.00" || len(result.Sections) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
}
