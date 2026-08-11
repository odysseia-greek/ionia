package bibliotheke

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/odysseia-greek/agora/aristoteles"
	arv1 "github.com/odysseia-greek/attike/aristophanes/gen/go/v1"
	v1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var ErrNotFound = errors.New("text not found")
var ErrMultipleTexts = errors.New("multiple texts found")

// Store captures the persistence required by the three migrated legacy
// operations. An Elasticsearch implementation can replace MemoryStore without
// changing the gRPC transport.
type Store interface {
	CreateText(context.Context, *v1.CreateTextRequest) (*v1.Text, error)
	Options(context.Context) (*v1.CorpusOptions, error)
}

type Service struct {
	v1.UnimplementedDiodorosServiceServer
	store    Store
	version  string
	Elastic  aristoteles.Client
	Index    string
	Streamer arv1.TraceService_ChorusClient
}

func NewService(store Store, version string) *Service {
	return &Service{store: store, version: version}
}

func (s *Service) Health(context.Context, *emptypb.Empty) (*v1.HealthResponse, error) {
	return &v1.HealthResponse{Healthy: true, Time: time.Now().UTC().Format(time.RFC3339), Version: s.version}, nil
}

func (s *Service) CreateText(ctx context.Context, req *v1.CreateTextRequest) (*v1.Text, error) {
	if req.GetAuthor() == "" || req.GetBook() == "" || req.GetReference() == "" {
		return nil, status.Error(codes.InvalidArgument, "author, book and reference are required")
	}
	text, err := s.store.CreateText(ctx, req)
	if errors.Is(err, ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "no text for author %q, book %q and reference %q", req.Author, req.Book, req.Reference)
	}
	if errors.Is(err, ErrMultipleTexts) {
		return nil, status.Errorf(codes.FailedPrecondition, "more than one text for author %q, book %q and reference %q", req.Author, req.Book, req.Reference)
	}
	return text, err
}

func (s *Service) Options(ctx context.Context, _ *emptypb.Empty) (*v1.CorpusOptions, error) {
	return s.store.Options(ctx)
}

func (s *Service) CheckText(ctx context.Context, req *v1.CheckTextRequest) (*v1.CheckTextResponse, error) {
	text, err := s.CreateText(ctx, &v1.CreateTextRequest{Author: req.Author, Book: req.Book, Reference: req.Reference})
	if err != nil {
		return nil, err
	}

	result := &v1.CheckTextResponse{}
	var total float64
	for _, answer := range req.GetTranslations() {
		for _, passage := range text.GetPassages() {
			if answer.GetSection() != passage.GetSection() {
				continue
			}
			for _, translation := range passage.GetTranslations() {
				percentage := similarity(translation, answer.GetTranslation())
				result.Sections = append(result.Sections, &v1.AnswerSection{Section: passage.Section, LevenshteinPercentage: fmt.Sprintf("%.2f", percentage), QuizSentence: translation, AnswerSentence: answer.Translation})
				if percentage < 100 {
					result.PossibleTypos = append(result.PossibleTypos, findTypos(answer.Translation, translation)...)
				}
				total += percentage
			}
		}
	}
	if len(result.Sections) > 0 {
		result.AverageLevenshteinPercentage = fmt.Sprintf("%.2f", total/float64(len(result.Sections)))
	} else {
		result.AverageLevenshteinPercentage = "0.00"
	}
	return result, nil
}

type MemoryStore struct {
	mu    sync.RWMutex
	texts []*v1.Text
}

func NewMemoryStore(texts ...*v1.Text) *MemoryStore { return &MemoryStore{texts: texts} }

func (m *MemoryStore) CreateText(_ context.Context, req *v1.CreateTextRequest) (*v1.Text, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, text := range m.texts {
		if !strings.EqualFold(text.Author, req.Author) || !strings.EqualFold(text.Book, req.Book) || !strings.EqualFold(text.Reference, req.Reference) {
			continue
		}
		if req.Section == "" {
			return cloneText(text, text.Passages), nil
		}
		for _, passage := range text.Passages {
			if passage.Section == req.Section {
				return cloneText(text, []*v1.Passage{passage}), nil
			}
		}
		return cloneText(text, text.Passages), nil
	}
	return nil, ErrNotFound
}

func (m *MemoryStore) Options(context.Context) (*v1.CorpusOptions, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := &v1.CorpusOptions{}
	authors := map[string]*v1.Author{}
	books := map[string]map[string]*v1.Book{}
	refs := map[string]map[string]map[string]*v1.Reference{}
	for _, text := range m.texts {
		author := authors[text.Author]
		if author == nil {
			author = &v1.Author{Name: text.Author}
			authors[text.Author] = author
			books[text.Author] = map[string]*v1.Book{}
			refs[text.Author] = map[string]map[string]*v1.Reference{}
			result.Authors = append(result.Authors, author)
		}
		book := books[text.Author][text.Book]
		if book == nil {
			book = &v1.Book{Name: text.Book}
			books[text.Author][text.Book] = book
			refs[text.Author][text.Book] = map[string]*v1.Reference{}
			author.Books = append(author.Books, book)
		}
		reference := refs[text.Author][text.Book][text.Reference]
		if reference == nil {
			reference = &v1.Reference{Name: text.Reference}
			refs[text.Author][text.Book][text.Reference] = reference
			book.References = append(book.References, reference)
		}
		for _, passage := range text.Passages {
			reference.Sections = append(reference.Sections, passage.Section)
		}
	}
	return result, nil
}

func cloneText(text *v1.Text, passages []*v1.Passage) *v1.Text {
	return &v1.Text{Author: text.Author, Book: text.Book, Type: text.Type, Reference: text.Reference, PerseusTextLink: text.PerseusTextLink, Passages: passages}
}

func similarity(source, provided string) float64 {
	longest := len(source)
	if len(provided) > longest {
		longest = len(provided)
	}
	if longest == 0 {
		return 100
	}
	return (1 - float64(levenshtein(source, provided))/float64(longest)) * 100
}

func levenshtein(a, b string) int {
	column := make([]int, len(a)+1)
	for i := range column {
		column[i] = i
	}
	for x := 1; x <= len(b); x++ {
		column[0] = x
		last := x - 1
		for y := 1; y <= len(a); y++ {
			old := column[y]
			cost := 0
			if a[y-1] != b[x-1] {
				cost = 1
			}
			column[y] = min(column[y]+1, column[y-1]+1, last+cost)
			last = old
		}
	}
	return column[len(a)]
}

func findTypos(provided, source string) []*v1.Typo {
	clean := func(value string) []string {
		return strings.Fields(strings.Map(func(r rune) rune {
			if strings.ContainsRune(",`~<>/?!.;:'\"", r) {
				return -1
			}
			return r
		}, value))
	}
	var result []*v1.Typo
	for _, given := range clean(provided) {
		exact := false
		start := len(result)
		for _, wanted := range clean(source) {
			distance := levenshtein(strings.ToLower(given), strings.ToLower(wanted))
			if distance == 0 {
				exact = true
				break
			}
			if distance == 1 || (distance <= 3 && len(wanted) > 10) {
				result = append(result, &v1.Typo{Source: wanted, Provided: given})
			}
		}
		if exact {
			result = result[:start]
		}
	}
	return result
}
