package app

import (
	"github.com/kpango/glg"
	"github.com/odysseia-greek/eupalinos"
	configs "github.com/odysseia-greek/ionia/parmenides/config"
	"github.com/odysseia-greek/plato/models"
	"strings"
	"sync"
)

type ParmenidesHandler struct {
	Config *configs.Config
}

func (p *ParmenidesHandler) DeleteIndexAtStartUp() error {
	deleted, err := p.Config.Elastic.Index().Delete(p.Config.Index)
	glg.Infof("deleted index: %s success: %v", p.Config.Index, deleted)
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

func (p *ParmenidesHandler) CreateIndexAtStartup() error {
	indexMapping := p.Config.Elastic.Builder().Index()
	created, err := p.Config.Elastic.Index().Create(p.Config.Index, indexMapping)
	if err != nil {
		return err
	}

	glg.Infof("created index: %s %v", created.Index, created.Acknowledged)

	return nil
}

func (p *ParmenidesHandler) Add(logoi models.Logos, wg *sync.WaitGroup, method, category string, lines chan eupalinos.Message) error {
	defer wg.Done()
	for _, word := range logoi.Logos {
		meros := models.Meros{
			Greek:      word.Greek,
			English:    word.Translation,
			LinkedWord: "",
			Original:   word.Greek,
		}

		if method == "mouseion" {
			meros.Dutch = word.Translation
			meros.English = ""
		}

		jsonsifiedMeros, _ := meros.Marshal()
		if lines != nil {
			lines <- jsonsifiedMeros
		}

		word.Category = category
		word.Method = method
		jsonifiedLogos, _ := word.Marshal()
		_, err := p.Config.Elastic.Index().CreateDocument(p.Config.Index, jsonifiedLogos)

		if err != nil {
			return err
		}

		glg.Infof("created word: %s with translation %s | method: %s | category: %s", word.Greek, word.Translation, word.Method, word.Category)

		p.Config.Created++
	}
	return nil
}
