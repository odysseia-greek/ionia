package app

import (
	"github.com/kpango/glg"
	"github.com/odysseia-greek/aristoteles"
	configs "github.com/odysseia-greek/ionia/herakleitos/config"
	"github.com/odysseia-greek/plato/models"
	"net/http"
	"sync"
)

type HerakleitosHandler struct {
	Config *configs.Config
}

func (h *HerakleitosHandler) DeleteIndexAtStartUp() error {
	deleted, err := h.Config.Elastic.Index().Delete(h.Config.Index)
	glg.Infof("deleted index: %s %v", h.Config.Index, deleted)
	if err != nil {
		glg.Error(err)
		b := []byte(err.Error())
		indexError, err := aristoteles.UnmarshalIndexError(b)
		if err != nil {
			return err
		}
		if indexError.Status == http.StatusNotFound {
			return nil
		}
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
