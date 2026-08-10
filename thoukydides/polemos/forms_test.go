package polemos

import (
	"context"
	"testing"

	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
)

type memoryForms struct{ forms []*v1.Form }

func (m memoryForms) List(context.Context, int) ([]*v1.Form, error) { return m.forms, nil }
func (m memoryForms) Get(_ context.Context, id string) (*v1.Form, error) {
	for _, form := range m.forms {
		if form.Id == id {
			return form, nil
		}
	}
	return nil, ErrFormNotFound
}

func TestServicePassesFormBlobThrough(t *testing.T) {
	want := &v1.Form{Id: "chapter-02", Blob: `{"grammar":[{"id":"nominative"}]}`}
	service := NewService("test", memoryForms{forms: []*v1.Form{want}})
	got, err := service.GetForm(context.Background(), &v1.GetFormRequest{Id: want.Id})
	if err != nil {
		t.Fatal(err)
	}
	if got.Blob != want.Blob {
		t.Fatalf("blob changed: %s", got.Blob)
	}
}
