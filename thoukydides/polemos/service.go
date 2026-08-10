package polemos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Service struct {
	v1.UnimplementedThoukydidesServiceServer
	mu       sync.RWMutex
	sessions map[string]*v1.ReadingSession
	version  string
	forms    FormStore
}

func NewService(version string, stores ...FormStore) *Service {
	var forms FormStore
	if len(stores) > 0 {
		forms = stores[0]
	}
	return &Service{sessions: make(map[string]*v1.ReadingSession), version: version, forms: forms}
}

func (s *Service) ListForms(ctx context.Context, req *v1.ListFormsRequest) (*v1.ListFormsResponse, error) {
	if s.forms == nil {
		return nil, status.Error(codes.Unavailable, "form store is not configured")
	}
	forms, err := s.forms.List(ctx, int(req.GetSize()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list forms: %v", err)
	}
	return &v1.ListFormsResponse{Forms: forms}, nil
}

func (s *Service) GetForm(ctx context.Context, req *v1.GetFormRequest) (*v1.Form, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if s.forms == nil {
		return nil, status.Error(codes.Unavailable, "form store is not configured")
	}
	form, err := s.forms.Get(ctx, req.Id)
	if err == ErrFormNotFound {
		return nil, status.Errorf(codes.NotFound, "form %q not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get form: %v", err)
	}
	return form, nil
}

func (s *Service) Health(context.Context, *emptypb.Empty) (*v1.HealthResponse, error) {
	return &v1.HealthResponse{Healthy: true, Time: time.Now().UTC().Format(time.RFC3339), Version: s.version}, nil
}

func (s *Service) StartReading(_ context.Context, req *v1.StartReadingRequest) (*v1.ReadingSession, error) {
	if req.GetUserId() == "" || req.GetAuthor() == "" || req.GetBook() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id, author and book are required")
	}
	id, err := newID()
	if err != nil {
		return nil, status.Error(codes.Internal, "could not create session id")
	}
	session := &v1.ReadingSession{Id: id, UserId: req.UserId, Author: req.Author, Book: req.Book, Reference: req.Reference, Section: req.Section, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	s.mu.Lock()
	s.sessions[id] = session
	s.mu.Unlock()
	return session, nil
}

func (s *Service) GetReading(_ context.Context, req *v1.GetReadingRequest) (*v1.ReadingSession, error) {
	s.mu.RLock()
	session, ok := s.sessions[req.GetId()]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "reading session %q not found", req.GetId())
	}
	return session, nil
}

func newID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("random id: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
