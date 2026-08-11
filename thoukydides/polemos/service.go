package polemos

import (
	"context"
	"errors"
	"time"

	"github.com/odysseia-greek/agora/aristoteles"
	arv1 "github.com/odysseia-greek/attike/aristophanes/gen/go/v1"
	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Service struct {
	v1.UnimplementedThoukydidesServiceServer
	version  string
	forms    FormStore
	Elastic  aristoteles.Client
	Index    string
	Streamer arv1.TraceService_ChorusClient
}

func NewService(version string, stores ...FormStore) *Service {
	var forms FormStore
	if len(stores) > 0 {
		forms = stores[0]
	}
	return &Service{version: version, forms: forms}
}

func (s *Service) Health(context.Context, *emptypb.Empty) (*v1.HealthResponse, error) {
	return &v1.HealthResponse{Healthy: true, Time: time.Now().UTC().Format(time.RFC3339), Version: s.version}, nil
}

func (s *Service) Options(ctx context.Context, _ *emptypb.Empty) (*v1.ChapterOptions, error) {
	if s.forms == nil {
		return nil, status.Error(codes.Unavailable, "chapter store is not configured")
	}
	chapters, err := s.forms.Options(ctx, 100)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list chapter options: %v", err)
	}
	return &v1.ChapterOptions{Chapters: chapters}, nil
}

func (s *Service) GetChapter(ctx context.Context, request *v1.GetChapterRequest) (*v1.Chapter, error) {
	if request.GetChapter() == "" {
		return nil, status.Error(codes.InvalidArgument, "chapter is required")
	}
	chapter, err := s.chapter(ctx, request.Chapter)
	if err != nil {
		return nil, err
	}
	public, err := publicChapter(chapter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "prepare chapter: %v", err)
	}
	return public, nil
}

func (s *Service) CheckChapter(ctx context.Context, request *v1.CheckChapterRequest) (*v1.CheckChapterResponse, error) {
	if request.GetChapter() == "" {
		return nil, status.Error(codes.InvalidArgument, "chapter is required")
	}
	chapter, err := s.chapter(ctx, request.Chapter)
	if err != nil {
		return nil, err
	}
	texts := make(map[string]chapterText, len(chapter.Texts))
	for _, text := range chapter.Texts {
		texts[text.ID] = text
	}
	result := &v1.CheckChapterResponse{Chapter: chapter.Chapter}
	for _, answer := range request.Answers {
		text, ok := texts[answer.GetText()]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "text %q is not part of chapter %q", answer.GetText(), chapter.Chapter)
		}
		result.Texts = append(result.Texts, &v1.CheckedText{Text: text.ID, SourceText: text.Greek, ActualText: text.Translation, LearnerText: answer.LearnerText})
	}
	return result, nil
}

func (s *Service) chapter(ctx context.Context, name string) (*chapterDocument, error) {
	if s.forms == nil {
		return nil, status.Error(codes.Unavailable, "chapter store is not configured")
	}
	chapter, err := s.forms.GetChapter(ctx, name)
	if errors.Is(err, ErrChapterNotFound) {
		return nil, status.Errorf(codes.NotFound, "chapter %q not found", name)
	}
	if errors.Is(err, ErrMultipleChapters) {
		return nil, status.Errorf(codes.FailedPrecondition, "multiple chapters found for %q", name)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get chapter: %v", err)
	}
	return chapter, nil
}
