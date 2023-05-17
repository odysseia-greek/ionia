package app

import (
	"github.com/kpango/glg"
	configs "github.com/odysseia-greek/ionia/herakleitos/config"
	"github.com/odysseia-greek/plato/models"
	"strings"
	"sync"
)

type HerakleitosHandler struct {
	Config *configs.Config
}

func (h *HerakleitosHandler) DeleteIndexAtStartUp() error {
	deleted, err := h.Config.Elastic.Index().Delete(h.Config.Index)
	glg.Infof("deleted index: %s success: %v", h.Config.Index, deleted)
	if err != nil {
		if deleted {
			return nil
		}
		if strings.Contains(err.Error(), "index_not_found_exception") {
			glg.Debug(err)
			return nil
		}

		return err
	}

	return nil
}

func (h *HerakleitosHandler) CreateIndexAtStartup() error {
	indexMapping := h.Config.Elastic.Builder().Index()
	created, err := h.Config.Elastic.Index().Create(h.Config.Index, indexMapping)
	if err != nil {
		return err
	}

	glg.Infof("created index: %s %v", created.Index, created.Acknowledged)

	return nil
}

func (h *HerakleitosHandler) Add(rhema models.Rhema, wg *sync.WaitGroup) error {
	defer wg.Done()
	for _, word := range rhema.Rhemai {
		jsonifiedLogos, _ := word.Marshal()
		_, err := h.Config.Elastic.Index().CreateDocument(h.Config.Index, jsonifiedLogos)

		if err != nil {
			return err
		}

		h.Config.Created++
	}
	return nil
}
