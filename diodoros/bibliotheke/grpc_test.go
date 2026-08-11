package bibliotheke

import (
	"context"
	"net"
	"testing"

	v1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestGRPCEndpoints(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	v1.RegisterDiodorosServiceServer(server, NewService(NewMemoryStore(fixture()), "test"))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := v1.NewDiodorosServiceClient(conn)
	ctx := context.Background()
	text, err := client.CreateText(ctx, &v1.CreateTextRequest{Author: "Herodotos", Book: "Histories", Reference: "1.1", Section: "a"})
	if err != nil || len(text.Passages) != 1 {
		t.Fatalf("CreateText: text=%v err=%v", text, err)
	}
	options, err := client.Options(ctx, &emptypb.Empty{})
	if err != nil || len(options.Authors) != 1 {
		t.Fatalf("Options: options=%v err=%v", options, err)
	}
	checked, err := client.CheckText(ctx, &v1.CheckTextRequest{Author: "Herodotos", Book: "Histories", Reference: "1.1", Translations: []*v1.TranslationAnswer{{Section: "a", Translation: "This is a sentence"}}})
	if err != nil || checked.AverageLevenshteinPercentage != "100.00" {
		t.Fatalf("CheckText: response=%v err=%v", checked, err)
	}
}
