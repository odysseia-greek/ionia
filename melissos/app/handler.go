package app

import (
	"context"
	"encoding/json"
	"github.com/kpango/glg"
	configs "github.com/odysseia-greek/ionia/melissos/config"
	"github.com/odysseia-greek/plato/models"
	"github.com/odysseia-greek/plato/transform"
	"github.com/segmentio/kafka-go"
	"os"
	"strconv"
	"strings"
	"sync"
)

type MelissosHandler struct {
	Config *configs.Config
}

func (m *MelissosHandler) getKafkaReader() *kafka.Reader {
	brokers := strings.Split(m.Config.KafkaUrl, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     "melissos",
		GroupTopics: []string{m.Config.Topic, m.Config.Mouseion},
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	})
}

func (m *MelissosHandler) getStartupCode() string {
	brokers := strings.Split(m.Config.KafkaUrl, ",")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       m.Config.Topic,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.FirstOffset,
	})
	exitCode, _ := r.ReadMessage(context.Background())
	r.Close()
	return string(exitCode.Key)
}

func (m *MelissosHandler) Handle() {
	exitCode := m.getStartupCode()
	glg.Infof("found exit code %s", exitCode)
	reader := m.getKafkaReader()
	mouseionClosed := false
	primaryClosed := false

	defer reader.Close()

	for {
		if mouseionClosed && primaryClosed {
			glg.Info("received exit code and shutting down")
			os.Exit(0)
		}

		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			glg.Error(err)
		}

		if string(msg.Key) == exitCode {
			exit, err := strconv.ParseBool(string(msg.Value))
			if err != nil {
				glg.Fatal("error reading exit codes")
			}

			if exit {
				switch msg.Topic {
				case m.Config.Topic:
					primaryClosed = true
				case m.Config.Mouseion:
					mouseionClosed = true
				}
			}

			continue
		}

		var word models.Meros
		err = json.Unmarshal(msg.Value, &word)
		if err != nil {
			glg.Error(err)
		}

		if msg.Topic == m.Config.Mouseion {
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
		} else {
			glg.Infof("word: %s already in dictionary", string(msg.Value))
		}
		glg.Infof("Topic: %s, Partition: %v Offset: %v Message: %s = %s", msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}
}

func (m *MelissosHandler) addDutchWord(word models.Meros) error {
	term := "dutch"
	query := m.Config.Elastic.Builder().MatchQuery(term, word.Dutch)
	response, err := m.Config.Elastic.Query().Match(m.Config.Index, query)
	if err != nil {
		glg.Error(err)
		return err
	}

	if len(response.Hits.Hits) != 0 {
		return nil
	}

	m.addWord(word)

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

	glg.Debugf("created root word: %s", word.Greek)

	return
}
