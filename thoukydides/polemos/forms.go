package polemos

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/odysseia-greek/agora/aristoteles"
	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
)

var ErrFormNotFound = errors.New("form not found")

type FormStore interface {
	List(context.Context, int) ([]*v1.Form, error)
	Get(context.Context, string) (*v1.Form, error)
}
type ElasticFormStore struct {
	Client aristoteles.Client
	Index  string
}

func (s *ElasticFormStore) List(ctx context.Context, size int) ([]*v1.Form, error) {
	if size <= 0 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	response, err := s.Client.Query().MatchWithSort(ctx, s.Index, "asc", "order", size, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	if err != nil {
		return nil, err
	}
	forms := make([]*v1.Form, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		blob, err := json.Marshal(hit.Source)
		if err != nil {
			return nil, err
		}
		forms = append(forms, &v1.Form{Id: hit.ID, Blob: string(blob)})
	}
	return forms, nil
}

func (s *ElasticFormStore) Get(ctx context.Context, id string) (*v1.Form, error) {
	response, err := s.Client.Query().GetById(ctx, s.Index, id)
	if err != nil {
		return nil, err
	}
	if response == nil || !response.Found {
		return nil, ErrFormNotFound
	}
	blob, err := json.Marshal(response.Source)
	if err != nil {
		return nil, err
	}
	return &v1.Form{Id: response.Id, Blob: string(blob)}, nil
}
