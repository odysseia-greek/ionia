package polemos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/odysseia-greek/agora/aristoteles"
	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
)

var ErrChapterNotFound = errors.New("chapter not found")
var ErrMultipleChapters = errors.New("multiple chapters found")

type chapterText struct {
	ID           string     `json:"id"`
	Title        string     `json:"title"`
	Type         string     `json:"type"`
	Source       textSource `json:"source"`
	Greek        string     `json:"greek"`
	ReadingHints []string   `json:"readingHints"`
	Translation  string     `json:"translation"`
}

type textSource struct {
	Author    string `json:"author"`
	Work      string `json:"work"`
	Reference string `json:"reference"`
	Dialect   string `json:"dialect"`
}

type grammarDocument struct {
	ID          string          `json:"id"`
	Title       string          `json:"title"`
	Explanation string          `json:"explanation"`
	Example     *grammarExample `json:"example"`
}

type grammarExample struct {
	Greek       string `json:"greek"`
	Translation string `json:"translation"`
	Note        string `json:"note"`
}

type vocabularyDocument struct {
	Greek       string `json:"greek"`
	Translation string `json:"translation"`
}

type chapterDocument struct {
	Chapter     string
	Title       string
	Description string
	Context     string
	Order       int32
	Level       int32
	Grammar     []grammarDocument
	Vocab       []vocabularyDocument
	Texts       []chapterText
}

type FormStore interface {
	Options(context.Context, int) ([]*v1.ChapterOption, error)
	GetChapter(context.Context, string) (*chapterDocument, error)
}

type ElasticFormStore struct {
	Client aristoteles.Client
	Index  string
}

func (s *ElasticFormStore) Options(ctx context.Context, size int) ([]*v1.ChapterOption, error) {
	if size <= 0 {
		size = 100
	}
	if size > 100 {
		size = 100
	}
	response, err := s.Client.Query().MatchWithSort(ctx, s.Index, "asc", "order", size, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	if err != nil {
		return nil, err
	}
	options := make([]*v1.ChapterOption, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		chapter, err := decodeChapter(hit.Source)
		if err != nil {
			return nil, err
		}
		options = append(options, &v1.ChapterOption{Chapter: chapter.Chapter, Title: chapter.Title, Order: chapter.Order, Level: chapter.Level})
	}
	return options, nil
}

func (s *ElasticFormStore) GetChapter(ctx context.Context, chapter string) (*chapterDocument, error) {
	query := map[string]any{"query": map[string]any{"term": map[string]any{"id": chapter}}}
	response, err := s.Client.Query().Match(ctx, s.Index, query)
	if err != nil {
		return nil, err
	}
	if len(response.Hits.Hits) == 0 {
		return nil, ErrChapterNotFound
	}
	if len(response.Hits.Hits) > 1 {
		return nil, ErrMultipleChapters
	}
	return decodeChapter(response.Hits.Hits[0].Source)
}

func decodeChapter(source map[string]any) (*chapterDocument, error) {
	body, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	var value struct {
		ID          string               `json:"id"`
		Title       string               `json:"title"`
		Description string               `json:"description"`
		Context     string               `json:"context"`
		Order       int32                `json:"order"`
		Level       int32                `json:"level"`
		Grammar     []grammarDocument    `json:"grammar"`
		Vocab       []vocabularyDocument `json:"vocabulary"`
		Texts       []chapterText        `json:"texts"`
	}
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, err
	}
	if value.ID == "" {
		return nil, fmt.Errorf("chapter has no domain id")
	}
	return &chapterDocument{Chapter: value.ID, Title: value.Title, Description: value.Description, Context: value.Context, Order: value.Order, Level: value.Level, Grammar: value.Grammar, Vocab: value.Vocab, Texts: value.Texts}, nil
}

func publicChapter(chapter *chapterDocument) (*v1.Chapter, error) {
	result := &v1.Chapter{
		Chapter: chapter.Chapter, Title: chapter.Title, Description: chapter.Description, Context: chapter.Context,
		Order: chapter.Order, Level: chapter.Level,
		Grammar:    make([]*v1.Grammar, 0, len(chapter.Grammar)),
		Vocabulary: make([]*v1.Vocabulary, 0, len(chapter.Vocab)),
		Texts:      make([]*v1.ChapterText, 0, len(chapter.Texts)),
	}
	for _, grammar := range chapter.Grammar {
		item := &v1.Grammar{Grammar: grammar.ID, Title: grammar.Title, Explanation: grammar.Explanation}
		if grammar.Example != nil {
			item.Example = &v1.GrammarExample{Greek: grammar.Example.Greek, Translation: grammar.Example.Translation, Note: grammar.Example.Note}
		}
		result.Grammar = append(result.Grammar, item)
	}
	for _, vocabulary := range chapter.Vocab {
		result.Vocabulary = append(result.Vocabulary, &v1.Vocabulary{Greek: vocabulary.Greek, Translation: vocabulary.Translation})
	}
	for _, text := range chapter.Texts {
		result.Texts = append(result.Texts, &v1.ChapterText{
			Text: text.ID, Title: text.Title, Type: text.Type, Greek: text.Greek,
			Source:       &v1.TextSource{Author: text.Source.Author, Work: text.Source.Work, Reference: text.Source.Reference, Dialect: text.Source.Dialect},
			ReadingHints: append([]string{}, text.ReadingHints...),
		})
	}
	return result, nil
}
