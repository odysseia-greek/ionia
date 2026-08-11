package polemos

import (
	"context"
	"net"
	"testing"

	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCEndpoints(t *testing.T) {
	form := &v1.Form{Id: "chapter-02", Blob: `{"id":"chapter-02"}`}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	v1.RegisterThoukydidesServiceServer(server, NewService("test", memoryForms{forms: []*v1.Form{form}}))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := v1.NewThoukydidesServiceClient(conn)
	ctx := context.Background()
	listed, err := client.ListForms(ctx, &v1.ListFormsRequest{Size: 10})
	if err != nil || len(listed.Forms) != 1 {
		t.Fatalf("ListForms: response=%v err=%v", listed, err)
	}
	got, err := client.GetForm(ctx, &v1.GetFormRequest{Id: form.Id})
	if err != nil || got.Blob != form.Blob {
		t.Fatalf("GetForm: response=%v err=%v", got, err)
	}
	session, err := client.StartReading(ctx, &v1.StartReadingRequest{UserId: "reader", FormId: form.Id})
	if err != nil || session.Form.Id != form.Id {
		t.Fatalf("StartReading: response=%v err=%v", session, err)
	}
	progress, err := client.SaveProgress(ctx, &v1.SaveProgressRequest{Id: session.Id, ProgressBlob: `{"step":2}`})
	if err != nil || progress.ProgressBlob != `{"step":2}` {
		t.Fatalf("SaveProgress: response=%v err=%v", progress, err)
	}
	loaded, err := client.GetReading(ctx, &v1.GetReadingRequest{Id: session.Id})
	if err != nil || loaded.ProgressBlob != progress.ProgressBlob {
		t.Fatalf("GetReading: response=%v err=%v", loaded, err)
	}
}
