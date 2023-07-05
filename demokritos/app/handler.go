package app

import (
	"fmt"
	"github.com/kpango/glg"
	configs "github.com/odysseia-greek/ionia/demokritos/config"
	"github.com/odysseia-greek/plato/models"
	"github.com/odysseia-greek/plato/transform"
	"strings"
	"sync"
	"time"
)

type DemokritosHandler struct {
	Config *configs.Config
}

func (d *DemokritosHandler) DeleteIndexAtStartUp() error {
	deleted, err := d.Config.Elastic.Index().Delete(d.Config.Index)
	glg.Infof("deleted index: %s success: %v", d.Config.Index, deleted)
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

func (d *DemokritosHandler) CreateIndexAtStartup() error {
	glg.Info(fmt.Sprintf("creating index: %s with min: %v and max: %v ngram", d.Config.Index, d.Config.MinNGram, d.Config.MaxNGram))
	query := d.Config.Elastic.Builder().DictionaryIndex(d.Config.MinNGram, d.Config.MaxNGram)
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
			d.Config.Created++
		}
	}
}

func (d *DemokritosHandler) PrintProgress(total int) {
	for {
		percentage := float64(d.Config.Created) / float64(total) * 100
		glg.Info(fmt.Sprintf("Progress: %d/%d documents created (%.2f%%)", d.Config.Created, total, percentage))
		time.Sleep(5 * time.Second)
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
		d.Config.Created++
	}
}
