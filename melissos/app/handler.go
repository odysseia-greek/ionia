package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kpango/glg"
	"github.com/odysseia-greek/eupalinos"
	configs "github.com/odysseia-greek/ionia/melissos/config"
	"github.com/odysseia-greek/plato/models"
	"github.com/odysseia-greek/plato/transform"
	"os"
	"strings"
	"sync"
	"time"
)

type MelissosHandler struct {
	Config *configs.Config
}

func (m *MelissosHandler) Handle() {
	var startupCode string
	ctx, done := context.WithCancel(context.Background())

	received := make(chan eupalinos.Message)
	go func() {
		m.Config.Queue.PubSub().Subscribe(m.Config.Queue.PubSub().Redial(ctx), received)
		done()
	}()

	for msg := range received {
		if strings.Contains(string(msg), "exitcode:") {
			exitCode := strings.Split(string(msg), " ")[1]
			if exitCode == startupCode {
				glg.Info("received exit code and shutting down")
				os.Exit(0)
			}
		}

		if strings.Contains(string(msg), "startup:") {
			startupCode = strings.Split(string(msg), " ")[1]
			glg.Info("received startup code")
			continue
		}

		var word models.Meros
		err := json.Unmarshal(msg, &word)
		if err != nil {
			glg.Error(err)
		}

		m.Config.Processed++

		if word.Dutch != "" {
			err := m.addDutchWord(word)
			if err != nil {
				glg.Error(err)
			}

			continue
		}

		found, err := m.queryWord(word)
		if err != nil {
			continue
		}
		if !found {
			m.addWord(word)
		}
	}
}

func (m *MelissosHandler) addDutchWord(word models.Meros) error {
	if word.Dutch == "de, het" {
		return nil
	}
	s := m.stripMouseionWords(word.Greek)
	strippedWord := transform.RemoveAccents(s)

	term := "greek"
	query := m.Config.Elastic.Builder().MatchQuery(term, strippedWord)
	response, err := m.Config.Elastic.Query().Match(m.Config.Index, query)

	if err != nil {
		return err
	}

	for _, hit := range response.Hits.Hits {
		jsonHit, _ := json.Marshal(hit.Source)
		meros, _ := models.UnmarshalMeros(jsonHit)
		if len(response.Hits.Hits) > 1 {
			if meros.Greek != strippedWord && meros.Greek != s {
				continue
			}
		}
		meros.Dutch = word.Dutch
		jsonifiedLogos, _ := meros.Marshal()
		_, err := m.Config.Elastic.Document().Update(m.Config.Index, hit.ID, jsonifiedLogos)
		if err != nil {
			return err
		}

		m.Config.Updated++
	}

	return nil
}

func (m *MelissosHandler) queryWord(word models.Meros) (bool, error) {
	found := false

	strippedWord := transform.RemoveAccents(word.Greek)

	term := "greek"
	query := m.Config.Elastic.Builder().MatchQuery(term, strippedWord)
	response, err := m.Config.Elastic.Query().Match(m.Config.Index, query)

	if err != nil {
		glg.Error(err)
		return found, err
	}

	if len(response.Hits.Hits) >= 1 {
		found = true
	}

	var parsedEnglishWord string
	pronouns := []string{"a", "an", "the"}
	splitEnglish := strings.Split(word.English, " ")
	numberOfWords := len(splitEnglish)
	if numberOfWords > 1 {
		for _, pronoun := range pronouns {
			if splitEnglish[0] == pronoun {
				toJoin := splitEnglish[1:numberOfWords]
				parsedEnglishWord = strings.Join(toJoin, " ")
				break
			} else {
				parsedEnglishWord = word.English
			}
		}
	} else {
		parsedEnglishWord = word.English
	}

	for _, hit := range response.Hits.Hits {
		jsonHit, _ := json.Marshal(hit.Source)
		meros, _ := models.UnmarshalMeros(jsonHit)
		if meros.English == parsedEnglishWord || meros.English == word.English {
			return true, nil
		} else {
			found = false
		}
	}

	return found, nil
}

func (m *MelissosHandler) addWord(word models.Meros) {
	var innerWaitGroup sync.WaitGroup
	jsonifiedLogos, _ := word.Marshal()
	_, err := m.Config.Elastic.Index().CreateDocument(m.Config.Index, jsonifiedLogos)

	if err != nil {
		glg.Error(err)
		return
	} else {
		innerWaitGroup.Add(1)
		go m.transformWord(word, &innerWaitGroup)
	}
}

func (m *MelissosHandler) transformWord(word models.Meros, wg *sync.WaitGroup) {
	defer wg.Done()
	strippedWord := transform.RemoveAccents(word.Greek)
	meros := models.Meros{
		Greek:      strippedWord,
		English:    word.English,
		Dutch:      word.Dutch,
		LinkedWord: word.LinkedWord,
		Original:   word.Greek,
	}

	jsonifiedLogos, _ := meros.Marshal()
	_, err := m.Config.Elastic.Index().CreateDocument(m.Config.Index, jsonifiedLogos)

	if err != nil {
		glg.Error(err)
		return
	}

	m.Config.Created++

	return
}

func (m *MelissosHandler) stripMouseionWords(word string) string {
	if !strings.Contains(word, " ") {
		return word
	}

	splitKeys := []string{"+", "("}
	greekPronous := []string{"ἡ", "ὁ", "τὸ", "τό"}
	var w string

	for _, key := range splitKeys {
		if strings.Contains(word, key) {
			split := strings.Split(word, key)
			w = strings.TrimSpace(split[0])
		}
	}

	for _, pronoun := range greekPronous {
		if strings.Contains(word, pronoun) {
			if strings.Contains(word, ",") {
				split := strings.Split(word, ",")
				w = strings.TrimSpace(split[0])
			} else {
				split := strings.Split(word, pronoun)
				w = strings.TrimSpace(split[1])
			}
		}
	}

	if w == "" {
		return word
	}

	return w
}

func (m *MelissosHandler) PrintProgress() {
	for {
		glg.Info(fmt.Sprintf("documents processed: %d | documents created: %d | documents updated: %d", m.Config.Processed, m.Config.Created, m.Config.Updated))
		time.Sleep(20 * time.Second)
	}
}
