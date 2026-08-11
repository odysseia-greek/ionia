package periodos

import (
	"context"
	"strings"

	"github.com/odysseia-greek/agora/aristoteles"
	"github.com/odysseia-greek/delphi/aristides/diplomat"
)

type Handler struct {
	Elastic    aristoteles.Client
	Index      string
	Ambassador *diplomat.ClientAmbassador
	Created    int
}

func (h *Handler) Put(ctx context.Context, form Form) error {
	if _, err := h.Elastic.Document().CreateWithId(ctx, h.Index, form.ID, form.Blob); err != nil {
		return err
	}
	h.Created++
	return nil
}

func (h *Handler) Reset(ctx context.Context) error {
	deleted, err := h.Elastic.Index().Delete(ctx, h.Index)
	if err != nil && !deleted && !strings.Contains(err.Error(), "index_not_found_exception") {
		return err
	}
	_, err = h.Elastic.Index().Create(ctx, h.Index, map[string]any{"mappings": map[string]any{"properties": map[string]any{"id": map[string]any{"type": "keyword"}, "order": map[string]any{"type": "integer"}, "level": map[string]any{"type": "integer"}, "title": map[string]any{"type": "text"}}}})
	return err
}
