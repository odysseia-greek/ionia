package polemos

import (
	"context"
	"net"
	"testing"

	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestGRPCEndpoints(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	v1.RegisterThoukydidesServiceServer(server, NewService("test", memoryForms{chapters: []*chapterDocument{fixtureChapter()}}))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := v1.NewThoukydidesServiceClient(conn)
	ctx := context.Background()
	options, err := client.Options(ctx, &emptypb.Empty{})
	if err != nil || len(options.Chapters) != 1 || options.Chapters[0].Chapter != "chapter-02" {
		t.Fatalf("Options: response=%v err=%v", options, err)
	}
	chapter, err := client.GetChapter(ctx, &v1.GetChapterRequest{Chapter: "chapter-02"})
	if err != nil || chapter.Chapter != "chapter-02" {
		t.Fatalf("GetChapter: response=%v err=%v", chapter, err)
	}
	checked, err := client.CheckChapter(ctx, &v1.CheckChapterRequest{Chapter: "chapter-02", Answers: []*v1.ChapterAnswer{{Text: "john-1-1", LearnerText: "At the start"}}})
	if err != nil || len(checked.Texts) != 1 || checked.Texts[0].ActualText != "In the beginning" {
		t.Fatalf("CheckChapter: response=%v err=%v", checked, err)
	}
}
