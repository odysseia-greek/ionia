package polemos

import (
	"context"
	"testing"

	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
)

type memoryForms struct{ chapters []*chapterDocument }

func (m memoryForms) Options(context.Context, int) ([]*v1.ChapterOption, error) {
	options := make([]*v1.ChapterOption, 0, len(m.chapters))
	for _, chapter := range m.chapters {
		options = append(options, &v1.ChapterOption{Chapter: chapter.Chapter, Title: chapter.Title, Order: chapter.Order, Level: chapter.Level})
	}
	return options, nil
}

func (m memoryForms) GetChapter(_ context.Context, name string) (*chapterDocument, error) {
	for _, chapter := range m.chapters {
		if chapter.Chapter == name {
			return chapter, nil
		}
	}
	return nil, ErrChapterNotFound
}

func fixtureChapter() *chapterDocument {
	return &chapterDocument{
		Chapter: "chapter-02", Title: "In the Beginning", Description: "Read your first text", Context: "The Gospel of John", Order: 2, Level: 2,
		Grammar: []grammarDocument{{ID: "nominative", Title: "The nominative"}},
		Vocab:   []vocabularyDocument{{Greek: "ἡ ἀρχή", Translation: "beginning"}},
		Texts:   []chapterText{{ID: "john-1-1", Greek: "Ἐν ἀρχῇ", ReadingHints: []string{"Find the verb"}, Translation: "In the beginning"}},
	}
}

func TestGetChapterDoesNotExposeAnswer(t *testing.T) {
	service := NewService("test", memoryForms{chapters: []*chapterDocument{fixtureChapter()}})
	chapter, err := service.GetChapter(context.Background(), &v1.GetChapterRequest{Chapter: "chapter-02"})
	if err != nil {
		t.Fatal(err)
	}
	if chapter.Description != "Read your first text" || chapter.Context != "The Gospel of John" {
		t.Fatalf("chapter context is incomplete: %#v", chapter)
	}
	if len(chapter.Grammar) != 1 || len(chapter.Vocabulary) != 1 || len(chapter.Texts) != 1 || len(chapter.Texts[0].ReadingHints) != 1 {
		t.Fatalf("chapter learning material is incomplete: %#v", chapter)
	}
}

func TestGetChapterReturnsEmptyLearningSlices(t *testing.T) {
	empty := &chapterDocument{Chapter: "chapter-01"}
	service := NewService("test", memoryForms{chapters: []*chapterDocument{empty}})
	chapter, err := service.GetChapter(context.Background(), &v1.GetChapterRequest{Chapter: "chapter-01"})
	if err != nil {
		t.Fatal(err)
	}
	if chapter.Grammar == nil || chapter.Vocabulary == nil || chapter.Texts == nil {
		t.Fatalf("expected empty slices, got %#v", chapter)
	}
}

func TestCheckChapterReturnsActualAndLearnerText(t *testing.T) {
	service := NewService("test", memoryForms{chapters: []*chapterDocument{fixtureChapter()}})
	result, err := service.CheckChapter(context.Background(), &v1.CheckChapterRequest{Chapter: "chapter-02", Answers: []*v1.ChapterAnswer{{Text: "john-1-1", LearnerText: "At the start"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Texts) != 1 || result.Texts[0].ActualText != "In the beginning" || result.Texts[0].LearnerText != "At the start" {
		t.Fatalf("unexpected check result: %#v", result)
	}
}
