package polemos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
)

var ErrFormNotFound = errors.New("form not found")

type FormStore interface {
	List(context.Context, int) ([]*v1.Form, error)
	Get(context.Context, string) (*v1.Form, error)
}

type ElasticFormStore struct {
	Address, Index, Username, Password string
	Client                             *http.Client
}

func (s *ElasticFormStore) List(ctx context.Context, size int) ([]*v1.Form, error) {
	if size <= 0 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	body, _ := json.Marshal(map[string]any{"size": size, "sort": []map[string]any{{"order": "asc"}}, "query": map[string]any{"match_all": map[string]any{}}})
	response, err := s.request(ctx, http.MethodPost, "/"+url.PathEscape(s.Index)+"/_search", body)
	if err != nil {
		return nil, err
	}
	var result struct {
		Hits struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return nil, err
	}
	forms := make([]*v1.Form, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		forms = append(forms, &v1.Form{Id: hit.ID, Blob: string(hit.Source)})
	}
	return forms, nil
}

func (s *ElasticFormStore) Get(ctx context.Context, id string) (*v1.Form, error) {
	response, err := s.request(ctx, http.MethodGet, "/"+url.PathEscape(s.Index)+"/_doc/"+url.PathEscape(id), nil)
	if err != nil {
		if errors.Is(err, ErrFormNotFound) {
			return nil, err
		}
		return nil, err
	}
	var result struct {
		ID     string          `json:"_id"`
		Source json.RawMessage `json:"_source"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return nil, err
	}
	return &v1.Form{Id: result.ID, Blob: string(result.Source)}, nil
}

func (s *ElasticFormStore) request(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.Address, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.Username != "" {
		req.SetBasicAuth(s.Username, s.Password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrFormNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("elasticsearch status %d: %s", resp.StatusCode, content)
	}
	return content, nil
}
