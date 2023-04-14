package app

import (
	"github.com/kpango/glg"
	"github.com/odysseia-greek/aristoteles"
	configs "github.com/odysseia-greek/ionia/demokritos/config"
	"github.com/odysseia-greek/plato/models"
	"github.com/odysseia-greek/plato/transform"
	"net/http"
	"sync"
)

type DemokritosHandler struct {
	Config *configs.Config
}

func (d *DemokritosHandler) DeleteIndexAtStartUp() error {
	deleted, err := d.Config.Elastic.Index().Delete(d.Config.Index)
	glg.Infof("deleted index: %s %v", d.Config.Index, deleted)
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

func (d *DemokritosHandler) CreateIndexAtStartup() error {
	query := d.Config.Elastic.Builder().SearchAsYouTypeIndex(d.Config.SearchWord)
	res, err := d.Config.Elastic.Index().Create(d.Config.Index, query)
	if err != nil {
		return err
	}

	glg.Infof("created index: %s", res.Index)
	return nil
}

func (d *DemokritosHandler) AddDirectoryToElastic(biblos models.Biblos, wg *sync.WaitGroup) {
	defer wg.Done()
	var innerWaitGroup sync.WaitGroup
	for _, word := range biblos.Biblos {
		jsonifiedWord, _ := word.Marshal()
		_, err := d.Config.Elastic.Index().CreateDocument(d.Config.Index, jsonifiedWord)

		if err != nil {
			glg.Error(err)
			return
		} else {
			innerWaitGroup.Add(1)
			go d.transformWord(word, &innerWaitGroup)
			glg.Debugf("created root word: %s", word.Greek)
			d.Config.Created++
		}
	}
}

func (d *DemokritosHandler) transformWord(m models.Meros, wg *sync.WaitGroup) {
	defer wg.Done()
	strippedWord := transform.RemoveAccents(m.Greek)
	word := models.Meros{
		Greek:      strippedWord,
		English:    m.English,
		LinkedWord: m.LinkedWord,
		Original:   m.Greek,
	}

	jsonifiedWord, _ := word.Marshal()
	_, err := d.Config.Elastic.Index().CreateDocument(d.Config.Index, jsonifiedWord)

	if err != nil {
		glg.Error(err)
		return
	} else {
		glg.Debugf("created parsed word: %s", strippedWord)
		d.Config.Created++
	}
}
