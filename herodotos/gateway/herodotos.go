package gateway

import (
	"context"
	"time"

	"github.com/odysseia-greek/agora/hesiodos"
	"github.com/odysseia-greek/agora/plato/config"
	arv1 "github.com/odysseia-greek/attike/aristophanes/gen/go/v1"
	"github.com/odysseia-greek/ionia/diodoros/bibliotheke"
	diodorosv1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	thoukydidesv1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"github.com/odysseia-greek/ionia/thoukydides/polemos"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

type HerodotosHandler struct {
	Core     *hesiodos.GenericGrpcClient[*polemos.Client]
	Corpus   *hesiodos.GenericGrpcClient[*bibliotheke.Client]
	Streamer arv1.TraceService_ChorusClient
}

func (h *HerodotosHandler) Close() {
	if h.Core != nil && h.Core.Client != nil {
		_ = h.Core.Client.Close()
	}
	if h.Corpus != nil && h.Corpus.Client != nil {
		_ = h.Corpus.Client.Close()
	}
}

func (h *HerodotosHandler) outgoingCtx(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	requestID, _ := parent.Value(config.HeaderKey).(string)
	sessionID, _ := parent.Value(config.SessionIdKey).(string)
	if requestID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, config.HeaderKey, requestID)
	}
	if sessionID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, config.SessionIdKey, sessionID)
	}
	return ctx, cancel
}

func (h *HerodotosHandler) CoreHealth(ctx context.Context) (response *thoukydidesv1.HealthResponse, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Core.CallWithReconnect(func(client *polemos.Client) error {
		response, err = client.Health(ctx, &emptypb.Empty{})
		return err
	})
	return
}

func (h *HerodotosHandler) CorpusHealth(ctx context.Context) (response *diodorosv1.HealthResponse, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Corpus.CallWithReconnect(func(client *bibliotheke.Client) error {
		response, err = client.Health(ctx, &emptypb.Empty{})
		return err
	})
	return
}

func (h *HerodotosHandler) ChapterOptions(ctx context.Context) (response *thoukydidesv1.ChapterOptions, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Core.CallWithReconnect(func(client *polemos.Client) error { response, err = client.Options(ctx, &emptypb.Empty{}); return err })
	return
}

func (h *HerodotosHandler) GetChapter(ctx context.Context, request *thoukydidesv1.GetChapterRequest) (response *thoukydidesv1.Chapter, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Core.CallWithReconnect(func(client *polemos.Client) error { response, err = client.GetChapter(ctx, request); return err })
	return
}

func (h *HerodotosHandler) CheckChapter(ctx context.Context, request *thoukydidesv1.CheckChapterRequest) (response *thoukydidesv1.CheckChapterResponse, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Core.CallWithReconnect(func(client *polemos.Client) error { response, err = client.CheckChapter(ctx, request); return err })
	return
}

func (h *HerodotosHandler) CreateText(ctx context.Context, request *diodorosv1.CreateTextRequest) (response *diodorosv1.Text, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Corpus.CallWithReconnect(func(client *bibliotheke.Client) error { response, err = client.CreateText(ctx, request); return err })
	return
}

func (h *HerodotosHandler) CorpusOptions(ctx context.Context) (response *diodorosv1.CorpusOptions, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Corpus.CallWithReconnect(func(client *bibliotheke.Client) error {
		response, err = client.Options(ctx, &emptypb.Empty{})
		return err
	})
	return
}

func (h *HerodotosHandler) CheckText(ctx context.Context, request *diodorosv1.CheckTextRequest) (response *diodorosv1.CheckTextResponse, err error) {
	ctx, cancel := h.outgoingCtx(ctx)
	defer cancel()
	err = h.Corpus.CallWithReconnect(func(client *bibliotheke.Client) error { response, err = client.CheckText(ctx, request); return err })
	return
}
