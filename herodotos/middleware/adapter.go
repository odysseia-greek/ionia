package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/odysseia-greek/agora/plato/config"
	"github.com/odysseia-greek/agora/plato/logging"
	"github.com/odysseia-greek/attike/aristophanes/comedy"
	arv1 "github.com/odysseia-greek/attike/aristophanes/gen/go/v1"
)

type Adapter func(http.Handler) http.Handler

func Adapt(handler http.Handler, adapters ...Adapter) http.Handler {
	for _, adapter := range adapters {
		handler = adapter(handler)
	}
	return handler
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

func LogRequestDetails(streamer arv1.TraceService_ChorusClient) Adapter {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := r.Header.Get(config.HeaderKey)
			sessionID := r.Header.Get(config.SessionIdKey)
			trace := comedy.TraceBareFromString(requestID)
			if trace.TraceId == "" || trace.SpanId == "" || !trace.Save || streamer == nil {
				ctx := context.WithValue(r.Context(), config.SessionIdKey, sessionID)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read request body", http.StatusInternalServerError)
				return
			}
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))

			var payload struct {
				OperationName string `json:"operationName"`
				Query         string `json:"query"`
			}
			_ = json.Unmarshal(body, &payload)
			if payload.OperationName == "" && payload.Query != "" {
				parts := strings.SplitN(payload.Query, "{", 2)
				if len(parts) == 2 {
					payload.OperationName = strings.TrimSpace(strings.SplitN(parts[1], "(", 2)[0])
				}
			}

			parentSpan := trace.SpanId
			graphqlSpan := comedy.GenerateSpanID()
			observation := &arv1.ObserveGraphQL{Operation: payload.OperationName, RootQuery: payload.Query}
			if err := streamer.Send(&arv1.ObserveRequest{TraceId: trace.TraceId, ParentSpanId: parentSpan, SpanId: graphqlSpan, Kind: &arv1.ObserveRequest_Graphql{Graphql: observation}}); err != nil {
				logging.Error(fmt.Sprintf("failed to send graphql trace data: %v", err))
			}

			trace.SpanId = graphqlSpan
			outgoingID := comedy.CreateCombinedId(trace)
			w.Header().Set(config.HeaderKey, outgoingID)
			w.Header().Set(config.SessionIdKey, sessionID)
			ctx := context.WithValue(r.Context(), config.HeaderKey, outgoingID)
			ctx = context.WithValue(ctx, config.SessionIdKey, sessionID)
			recorder := &statusRecorder{ResponseWriter: w}
			started := time.Now()
			next.ServeHTTP(recorder, r.WithContext(ctx))
			status := recorder.status
			if status == 0 {
				status = http.StatusOK
			}
			stop := &arv1.ObserveRequest{TraceId: trace.TraceId, ParentSpanId: parentSpan, SpanId: graphqlSpan, Kind: &arv1.ObserveRequest_TraceHopStop{TraceHopStop: &arv1.ObserveTraceHopStop{ResponseCode: int32(status), TookMs: time.Since(started).Milliseconds()}}}
			if err := streamer.Send(stop); err != nil {
				logging.Error(fmt.Sprintf("failed to close graphql trace hop: %v", err))
			}
		})
	}
}
