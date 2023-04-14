package app

import (
	"context"
	"fmt"
	"github.com/kpango/glg"
	"github.com/odysseia-greek/aristoteles"
	configs "github.com/odysseia-greek/ionia/parmenides/config"
	"github.com/odysseia-greek/plato/models"
	"github.com/segmentio/kafka-go"
	"net"
	"net/http"
	"strconv"
	"sync"
)

type ParmenidesHandler struct {
	Config *configs.Config
}

func (p *ParmenidesHandler) DeleteIndexAtStartUp() error {
	deleted, err := p.Config.Elastic.Index().Delete(p.Config.Index)
	glg.Infof("deleted index: %s %v", p.Config.Index, deleted)
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

func (p *ParmenidesHandler) CreateIndexAtStartup() error {
	indexMapping := p.Config.Elastic.Builder().Index()
	created, err := p.Config.Elastic.Index().Create(p.Config.Index, indexMapping)
	if err != nil {
		return err
	}

	glg.Infof("created index: %s %v", created.Index, created.Acknowledged)

	return nil
}

func (p *ParmenidesHandler) CreateTopicAtStartup() error {
	conn, err := kafka.Dial("tcp", p.Config.KafkaUrl)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}

	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             p.Config.Topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		{
			Topic:             p.Config.Mouseion,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	return controllerConn.CreateTopics(topicConfigs...)

}

func (p *ParmenidesHandler) Add(logoi models.Logos, wg *sync.WaitGroup, method, category string) error {
	defer wg.Done()
	for i, word := range logoi.Logos {
		meros := models.Meros{
			Greek:      word.Greek,
			English:    word.Translation,
			LinkedWord: "",
			Original:   word.Greek,
		}

		if method == p.Config.Mouseion {
			meros.Dutch = word.Translation
			meros.English = ""
		}

		err := p.queueWord(meros, i, method, category)
		if err != nil {
			glg.Error(err)
		}

		word.Category = category
		word.Method = method
		jsonifiedLogos, _ := word.Marshal()
		_, err = p.Config.Elastic.Index().CreateDocument(p.Config.Index, jsonifiedLogos)

		if err != nil {
			return err
		}

		glg.Infof("created word: %s with translation %s | method: %s | category: %s", word.Greek, word.Translation, word.Method, word.Category)

		p.Config.Created++
	}
	return nil
}

func (p *ParmenidesHandler) queueWord(meros models.Meros, index int, method, category string) error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(p.Config.KafkaUrl),
		Topic:    p.Config.Topic,
		Balancer: &kafka.LeastBytes{},
	}

	if meros.Dutch != "" {
		w.Topic = p.Config.Mouseion
	}

	defer w.Close()

	marshalled, _ := meros.Marshal()

	data := fmt.Sprintf("method: %s category: %s index: %v", method, category, index)
	return w.WriteMessages(context.Background(),
		kafka.Message{
			Key:        []byte(meros.Greek),
			Value:      marshalled,
			WriterData: data,
		},
	)
}

func (p *ParmenidesHandler) StartQueue() error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(p.Config.KafkaUrl),
		Topic:    p.Config.Topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer w.Close()

	glg.Debug("generating exit code at startup")

	return w.WriteMessages(context.Background(),
		kafka.Message{
			Key:    []byte(p.Config.ExitCode),
			Value:  []byte("false"),
			Offset: 0,
		},
	)
}

func (p *ParmenidesHandler) ExitQueue() error {
	w := &kafka.Writer{
		Addr:     kafka.TCP(p.Config.KafkaUrl),
		Topic:    p.Config.Topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer w.Close()

	glg.Debug("sending exit code after run")

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(p.Config.ExitCode),
			Value: []byte("true"),
		},
	)

	if err != nil {
		return err
	}

	sw := &kafka.Writer{
		Addr:     kafka.TCP(p.Config.KafkaUrl),
		Topic:    p.Config.Mouseion,
		Balancer: &kafka.LeastBytes{},
	}

	defer sw.Close()

	glg.Debug("sending exit code after run")

	return sw.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(p.Config.ExitCode),
			Value: []byte("true"),
		},
	)
}
