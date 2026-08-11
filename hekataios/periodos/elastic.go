package periodos

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/odysseia-greek/agora/aristoteles"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/delphi/aristides/diplomat"
)

type Handler struct {
	Elastic    aristoteles.Client
	Index      string
	Ambassador *diplomat.ClientAmbassador
	Created    int
}

func (h *Handler) Put(ctx context.Context, form Form) error {
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if _, err := h.Elastic.Index().CreateDocument(writeCtx, h.Index, form.Blob); err != nil {
		return fmt.Errorf("index form %s: %w", form.ID, err)
	}
	h.Created++
	return nil
}

func (h *Handler) DeleteIndexAtStartUp(ctx context.Context) error {
	deleteCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	deleted, err := h.Elastic.Index().Delete(deleteCtx, h.Index)
	logging.Info(fmt.Sprintf("deleted index: %s success: %v", h.Index, deleted))
	if err != nil {
		if deleted {
			return nil
		}
		if strings.Contains(err.Error(), "index_not_found_exception") {
			logging.Info(fmt.Sprintf("index %s did not exist; continuing", h.Index))
			return nil
		}
		return fmt.Errorf("delete index %s: %w", h.Index, err)
	}
	return nil
}

func (h *Handler) CreateIndexAtStartup(ctx context.Context) error {
	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	created, err := h.Elastic.Index().Create(createCtx, h.Index, formIndex())
	if err != nil {
		return fmt.Errorf("create index %s: %w", h.Index, err)
	}
	logging.Info(fmt.Sprintf("created index: %s acknowledged: %v", created.Index, created.Acknowledged))
	return nil
}

func (h *Handler) Reset(ctx context.Context) error {
	if err := h.DeleteIndexAtStartUp(ctx); err != nil {
		return err
	}
	return h.CreateIndexAtStartup(ctx)
}

func formIndex() map[string]any {
	return map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"id":    map[string]any{"type": "keyword"},
				"order": map[string]any{"type": "integer"},
				"level": map[string]any{"type": "integer"},
				"title": map[string]any{"type": "text"},
			},
		},
	}
}
