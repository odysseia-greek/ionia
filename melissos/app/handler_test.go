package app

import (
	elastic "github.com/odysseia-greek/aristoteles"
	configs "github.com/odysseia-greek/ionia/melissos/config"
	"github.com/odysseia-greek/plato/models"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestHandlerCreateDocuments(t *testing.T) {
	index := "test"

	body := models.Meros{
		Greek:      "ἀγορά",
		English:    "market place",
		LinkedWord: "",
		Original:   "",
	}

	t.Run("WordIsTheSame", func(t *testing.T) {
		file := "thalesSingleHit"
		status := 200
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		body.English = "a market place"

		testHandler := MelissosHandler{Config: &testConfig}
		found, err := testHandler.queryWord(body)
		assert.True(t, found)
		assert.Nil(t, err)
	})

	t.Run("WordWithAPronoun", func(t *testing.T) {
		file := "thalesSingleHit"
		status := 200
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		testHandler := MelissosHandler{Config: &testConfig}
		found, err := testHandler.queryWord(body)
		assert.True(t, found)
		assert.Nil(t, err)
	})

	t.Run("WordFoundButDifferentMeaning", func(t *testing.T) {
		file := "thalesSingleHit"
		status := 200
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		body.English = "notthesame"

		testHandler := MelissosHandler{Config: &testConfig}
		found, err := testHandler.queryWord(body)
		assert.False(t, found)
		assert.Nil(t, err)
	})

	t.Run("WordFoundDifferentMeaningWithoutAPronoun", func(t *testing.T) {
		file := "thalesSingleHit"
		status := 200
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		body.English = "notthesame but multiple words"

		testHandler := MelissosHandler{Config: &testConfig}
		found, err := testHandler.queryWord(body)
		assert.False(t, found)
		assert.Nil(t, err)
	})

	t.Run("DoesNotExist", func(t *testing.T) {
		file := "searchWordNoResults"
		status := 200
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		testHandler := MelissosHandler{Config: &testConfig}
		found, err := testHandler.queryWord(body)
		assert.False(t, found)
		assert.Nil(t, err)
	})

	t.Run("DoesNotExist", func(t *testing.T) {
		file := "shardFailure"
		status := 502
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		testHandler := MelissosHandler{Config: &testConfig}
		found, err := testHandler.queryWord(body)
		assert.False(t, found)
		assert.NotNil(t, err)
	})
}

func TestHandlerAddWord(t *testing.T) {
	index := "test"
	body := models.Meros{
		Greek:      "ἀγορά",
		English:    "a market place",
		LinkedWord: "",
		Original:   "",
	}

	t.Run("DocumentNotCreated", func(t *testing.T) {
		file := "shardFailure"
		status := 502
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		testHandler := MelissosHandler{Config: &testConfig}
		testHandler.addWord(body)
		assert.Equal(t, testConfig.Created, 0)
	})
}

func TestHandlerTransform(t *testing.T) {
	index := "test"
	body := models.Meros{
		Greek:      "ἀγορά",
		English:    "a market place",
		LinkedWord: "",
		Original:   "",
	}

	t.Run("DocumentCreated", func(t *testing.T) {
		file := "createDocument"
		status := 200
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		var wait sync.WaitGroup

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		wait.Add(1)

		testHandler := MelissosHandler{Config: &testConfig}
		testHandler.transformWord(body, &wait)
		assert.Equal(t, testConfig.Created, 1)
	})

	t.Run("DocumentNotCreated", func(t *testing.T) {
		file := "shardFailure"
		status := 502
		mockElasticClient, err := elastic.NewMockClient(file, status)
		assert.Nil(t, err)

		var wait sync.WaitGroup

		testConfig := configs.Config{
			Elastic: mockElasticClient,
			Index:   index,
			Created: 0,
		}

		wait.Add(1)

		testHandler := MelissosHandler{Config: &testConfig}
		testHandler.transformWord(body, &wait)
		assert.Equal(t, testConfig.Created, 0)
	})

}

func TestHandlerParser(t *testing.T) {
	splitWord := "ἀκούω + gen."
	pronounSplit := "μῦθος, ὁ"
	pronounSplitTwo := "ὁ δοῦλος"
	testConfig := configs.Config{
		Elastic: nil,
		Index:   "",
		Created: 0,
	}

	testHandler := MelissosHandler{Config: &testConfig}

	t.Run("SplitWordsWithPlus", func(t *testing.T) {
		sut := "ἀκούω"
		parsedWord := testHandler.stripMouseionWords(splitWord)
		assert.Equal(t, sut, parsedWord)
	})

	t.Run("SplitWordsWithPronoun", func(t *testing.T) {
		sut := "μῦθος"
		parsedWord := testHandler.stripMouseionWords(pronounSplit)
		assert.Equal(t, sut, parsedWord)
	})

	t.Run("SplitWordsWithPronounWithoutComma", func(t *testing.T) {
		sut := "δοῦλος"
		parsedWord := testHandler.stripMouseionWords(pronounSplitTwo)
		assert.Equal(t, sut, parsedWord)
	})
}
