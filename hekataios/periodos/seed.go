package periodos

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"
)

// Form is the stable envelope around an intentionally opaque chapter design.
// Blob preserves the complete source JSON for Thoukydides and Herodotos.
type Form struct {
	ID   string
	Blob json.RawMessage
}

type Sink interface {
	Put(context.Context, Form) error
}

func Load(ctx context.Context, source fs.FS, root string, sink Sink) (int, error) {
	forms := make(map[string]Form)
	order := make([]string, 0)
	err := fs.WalkDir(source, root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			return nil
		}
		content, err := fs.ReadFile(source, path)
		if err != nil {
			return err
		}
		var documents []json.RawMessage
		if err := json.Unmarshal(content, &documents); err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		for _, blob := range documents {
			var identity struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(blob, &identity); err != nil {
				return fmt.Errorf("parse identity in %s: %w", path, err)
			}
			if identity.ID == "" {
				return fmt.Errorf("form in %s has no id", path)
			}
			if _, exists := forms[identity.ID]; !exists {
				order = append(order, identity.ID)
			}
			forms[identity.ID] = Form{ID: identity.ID, Blob: blob}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, id := range order {
		if err := sink.Put(ctx, forms[id]); err != nil {
			return 0, fmt.Errorf("seed %s: %w", id, err)
		}
	}
	return len(forms), nil
}
