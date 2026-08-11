package polemos

import (
	"context"
	"fmt"
	v1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"
)

const DefaultAddress = "localhost:50060"

type Client struct {
	service v1.ThoukydidesServiceClient
	conn    *grpc.ClientConn
}

func NewClient(address string) (*Client, error) {
	if address == "" {
		address = DefaultAddress
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create Thoukydides client: %w", err)
	}
	return &Client{service: v1.NewThoukydidesServiceClient(conn), conn: conn}, nil
}
func (c *Client) Close() error { return c.conn.Close() }
func (c *Client) WaitForHealthyState() bool {
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		response, err := c.Health(context.Background(), &emptypb.Empty{})
		if err == nil && response.Healthy {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}
func (c *Client) Health(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*v1.HealthResponse, error) {
	return c.service.Health(ctx, req, opts...)
}
func (c *Client) Options(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*v1.ChapterOptions, error) {
	return c.service.Options(ctx, req, opts...)
}
func (c *Client) GetChapter(ctx context.Context, req *v1.GetChapterRequest, opts ...grpc.CallOption) (*v1.Chapter, error) {
	return c.service.GetChapter(ctx, req, opts...)
}
func (c *Client) CheckChapter(ctx context.Context, req *v1.CheckChapterRequest, opts ...grpc.CallOption) (*v1.CheckChapterResponse, error) {
	return c.service.CheckChapter(ctx, req, opts...)
}
