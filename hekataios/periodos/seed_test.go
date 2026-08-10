package periodos

import (
	"context"
	"testing"
	"testing/fstest"
)

type collectingSink []Form

func (s *collectingSink) Put(_ context.Context, form Form) error { *s = append(*s, form); return nil }

func TestLoadPreservesFormAsBlob(t *testing.T) {
	source := fstest.MapFS{"rhema/chapter.json": {Data: []byte(`[{"id":"chapter-01","grammar":[{"id":"noun"}]}]`)}}
	var sink collectingSink
	count, err := Load(context.Background(), source, "rhema", &sink)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(sink) != 1 || sink[0].ID != "chapter-01" {
		t.Fatalf("count=%d forms=%#v", count, sink)
	}
	if string(sink[0].Blob) != `{"id":"chapter-01","grammar":[{"id":"noun"}]}` {
		t.Fatalf("blob changed: %s", sink[0].Blob)
	}
}
