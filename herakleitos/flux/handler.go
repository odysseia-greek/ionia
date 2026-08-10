package flux

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	elastic "github.com/odysseia-greek/agora/aristoteles"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/delphi/aristides/diplomat"
)

type HerakleitosHandler struct {
	Index      string
	Created    int
	Elastic    elastic.Client
	PolicyName string
	Ambassador *diplomat.ClientAmbassador
}

func (h *HerakleitosHandler) DeleteIndexAtStartUp() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deleted, err := h.Elastic.Index().DeleteWithContext(ctx, h.Index)
	logging.Info(fmt.Sprintf("deleted index: %s success: %v", h.Index, deleted))
	if err != nil {
		if deleted {
			return nil
		}
		if strings.Contains(err.Error(), "index_not_found_exception") {
			logging.Error(err.Error())
			return nil
		}
		return err
	}

	return nil
}

func (h *HerakleitosHandler) CreateIndexAtStartup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	indexMapping := textIndex(h.PolicyName)
	created, err := h.Elastic.Index().CreateWithContext(ctx, h.Index, indexMapping)
	if err != nil {
		return err
	}

	logging.Info(fmt.Sprintf("created index: %s %v", created.Index, created.Acknowledged))

	return nil
}

func (h *HerakleitosHandler) Add(rhemai []Text, wg *sync.WaitGroup) error {
	defer wg.Done()
	var buf bytes.Buffer

	for _, rhema := range rhemai {
		meta := []byte(fmt.Sprintf(`{ "index": {} }%s`, "\n"))
		jsonifiedText, err := json.Marshal(rhema)
		if err != nil {
			return err
		}
		jsonifiedText = append(jsonifiedText, "\n"...)
		buf.Grow(len(meta) + len(jsonifiedText))
		buf.Write(meta)
		buf.Write(jsonifiedText)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := h.Elastic.Document().BulkWithContext(ctx, buf, h.Index)
	if err != nil {
		return err
	}

	h.Created = h.Created + len(res.Items)

	return nil
}
