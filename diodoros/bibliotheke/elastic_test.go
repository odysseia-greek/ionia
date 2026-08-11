package bibliotheke

import (
	"encoding/json"
	"testing"
)

func TestTextAggregationQueryMatchesLegacyHandler(t *testing.T) {
	query := textAggregationQuery()
	authors := query["aggs"].(map[string]any)["authors"].(map[string]any)
	if field := authors["terms"].(map[string]any)["field"]; field != "author.keyword" {
		t.Fatalf("unexpected author aggregation field: %v", field)
	}
	books := authors["aggs"].(map[string]any)["books"].(map[string]any)
	references := books["aggs"].(map[string]any)["references"].(map[string]any)
	sections := references["aggs"].(map[string]any)["sections"].(map[string]any)
	if path := sections["nested"].(map[string]any)["path"]; path != "rhemai" {
		t.Fatalf("unexpected nested path: %v", path)
	}
	sectionIDs := sections["aggs"].(map[string]any)["section_ids"].(map[string]any)
	if field := sectionIDs["terms"].(map[string]any)["field"]; field != "rhemai.section" {
		t.Fatalf("unexpected section aggregation field: %v", field)
	}
}

func TestParseLegacyAggregationResults(t *testing.T) {
	fixture := map[string]any{
		"aggregations": map[string]any{
			"authors": map[string]any{"buckets": []any{
				map[string]any{"key": "Herodotos", "books": map[string]any{"buckets": []any{
					map[string]any{"key": "Histories", "references": map[string]any{"buckets": []any{
						map[string]any{"key": "1.1", "sections": map[string]any{"section_ids": map[string]any{"buckets": []any{
							map[string]any{"key": "a"}, map[string]any{"key": "b"},
						}}}},
					}}},
				}}},
			}},
		},
	}
	body, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	options, err := parseAggregationResults(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.Authors) != 1 || options.Authors[0].Name != "Herodotos" {
		t.Fatalf("unexpected authors: %#v", options.Authors)
	}
	reference := options.Authors[0].Books[0].References[0]
	if reference.Name != "1.1" || len(reference.Sections) != 2 || reference.Sections[1] != "b" {
		t.Fatalf("unexpected reference: %#v", reference)
	}
}
