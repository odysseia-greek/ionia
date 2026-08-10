package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type request struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}
type response struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func Execute(ctx context.Context, url, query string, variables map[string]any, target any) error {
	body, err := json.Marshal(&request{Query: query, Variables: variables})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		content, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("graphql http status %d: %s", resp.StatusCode, content)
	}
	var envelope response
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("graphql error: %s", envelope.Errors[0].Message)
	}
	if target == nil {
		return nil
	}
	return json.Unmarshal(envelope.Data, target)
}
