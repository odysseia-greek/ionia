package gateway

import (
	"context"
	"testing"

	"github.com/odysseia-greek/agora/plato/config"
	"google.golang.org/grpc/metadata"
)

func TestOutgoingContextPropagatesRequestMetadata(t *testing.T) {
	parent := context.WithValue(context.Background(), config.HeaderKey, "trace-id")
	parent = context.WithValue(parent, config.SessionIdKey, "session-id")
	ctx, cancel := (&HerodotosHandler{}).outgoingCtx(parent)
	defer cancel()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get(config.HeaderKey); len(got) != 1 || got[0] != "trace-id" {
		t.Fatalf("unexpected request metadata: %v", got)
	}
	if got := md.Get(config.SessionIdKey); len(got) != 1 || got[0] != "session-id" {
		t.Fatalf("unexpected session metadata: %v", got)
	}
}
