package periodos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type ElasticSink struct {
	Address, Index, Username, Password string
	Client                             *http.Client
}

func (s *ElasticSink) Put(ctx context.Context, form Form) error {
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	endpoint := strings.TrimRight(s.Address, "/") + "/" + url.PathEscape(s.Index) + "/_doc/" + url.PathEscape(form.ID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(form.Blob))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.Username != "" {
		req.SetBasicAuth(s.Username, s.Password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch status %d: %s", resp.StatusCode, body)
	}
	return nil
}
