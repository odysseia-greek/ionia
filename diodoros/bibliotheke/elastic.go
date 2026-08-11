package bibliotheke

import (
	"context"
	"encoding/json"
	"time"

	"github.com/odysseia-greek/agora/aristoteles"
	v1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
)

const elasticTimeout = 10 * time.Second

type ElasticStore struct {
	Client aristoteles.Client
	Index  string
}

type indexedText struct {
	Author          string        `json:"author"`
	Book            string        `json:"book"`
	Type            string        `json:"type"`
	Reference       string        `json:"reference"`
	PerseusTextLink string        `json:"perseusTextLink"`
	Rhemai          []*v1.Passage `json:"rhemai"`
}

func decodeText(source map[string]any) (*v1.Text, error) {
	blob, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	var indexed indexedText
	if err := json.Unmarshal(blob, &indexed); err != nil {
		return nil, err
	}
	return &v1.Text{Author: indexed.Author, Book: indexed.Book, Type: indexed.Type, Reference: indexed.Reference, PerseusTextLink: indexed.PerseusTextLink, Passages: indexed.Rhemai}, nil
}

func (s *ElasticStore) CreateText(ctx context.Context, req *v1.CreateTextRequest) (*v1.Text, error) {
	ctx, cancel := context.WithTimeout(ctx, elasticTimeout)
	defer cancel()

	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"must": []map[string]any{
					{"match": map[string]any{"author": map[string]any{"query": req.Author, "operator": "and", "fuzziness": "AUTO", "prefix_length": 0}}},
					{"match": map[string]any{"book": req.Book}},
					{"match": map[string]any{"reference": req.Reference}},
				},
			},
		},
	}
	response, err := s.Client.Query().Match(ctx, s.Index, query)
	if err != nil {
		return nil, err
	}
	if len(response.Hits.Hits) == 0 {
		return nil, ErrNotFound
	}
	if len(response.Hits.Hits) > 1 {
		return nil, ErrMultipleTexts
	}
	text, err := decodeText(response.Hits.Hits[0].Source)
	if err != nil {
		return nil, err
	}
	if req.Section != "" {
		for _, passage := range text.Passages {
			if passage.Section == req.Section {
				text.Passages = []*v1.Passage{passage}
				break
			}
		}
	}
	return text, nil
}

func (s *ElasticStore) Options(ctx context.Context) (*v1.CorpusOptions, error) {
	ctx, cancel := context.WithTimeout(ctx, elasticTimeout)
	defer cancel()

	response, err := s.Client.Query().MatchRaw(ctx, s.Index, textAggregationQuery())
	if err != nil {
		return nil, err
	}
	return parseAggregationResults(response)
}

func textAggregationQuery() map[string]any {
	return map[string]any{
		"size": 0,
		"aggs": map[string]any{
			"authors": map[string]any{
				"terms": map[string]any{"field": "author.keyword", "size": 100},
				"aggs": map[string]any{
					"books": map[string]any{
						"terms": map[string]any{"field": "book.keyword", "size": 100},
						"aggs": map[string]any{
							"references": map[string]any{
								"terms": map[string]any{"field": "reference", "size": 100},
								"aggs": map[string]any{
									"sections": map[string]any{
										"nested": map[string]any{"path": "rhemai"},
										"aggs":   map[string]any{"section_ids": map[string]any{"terms": map[string]any{"field": "rhemai.section", "size": 100}}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

type aggregationBucket struct {
	Key   string `json:"key"`
	Books struct {
		Buckets []aggregationBucket `json:"buckets"`
	} `json:"books"`
	References struct {
		Buckets []aggregationBucket `json:"buckets"`
	} `json:"references"`
	Sections struct {
		SectionIDs struct {
			Buckets []aggregationBucket `json:"buckets"`
		} `json:"section_ids"`
	} `json:"sections"`
}

func parseAggregationResults(response []byte) (*v1.CorpusOptions, error) {
	var raw struct {
		Aggregations struct {
			Authors struct {
				Buckets []aggregationBucket `json:"buckets"`
			} `json:"authors"`
		} `json:"aggregations"`
	}
	if err := json.Unmarshal(response, &raw); err != nil {
		return nil, err
	}
	result := &v1.CorpusOptions{}
	for _, authorBucket := range raw.Aggregations.Authors.Buckets {
		author := &v1.Author{Name: authorBucket.Key}
		for _, bookBucket := range authorBucket.Books.Buckets {
			book := &v1.Book{Name: bookBucket.Key}
			for _, referenceBucket := range bookBucket.References.Buckets {
				reference := &v1.Reference{Name: referenceBucket.Key}
				for _, sectionBucket := range referenceBucket.Sections.SectionIDs.Buckets {
					reference.Sections = append(reference.Sections, sectionBucket.Key)
				}
				book.References = append(book.References, reference)
			}
			author.Books = append(author.Books, book)
		}
		result.Authors = append(result.Authors, author)
	}
	return result, nil
}

var _ Store = (*ElasticStore)(nil)
