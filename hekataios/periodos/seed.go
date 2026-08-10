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
	count := 0
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
			if err := sink.Put(ctx, Form{ID: identity.ID, Blob: blob}); err != nil {
				return fmt.Errorf("seed %s: %w", path, err)
			}
			count++
		}
		return nil
	})
	return count, err
}
