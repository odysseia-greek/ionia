package bibliotheke

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const DefaultAddress = "localhost:50060"

type Client struct {
	service v1.DiodorosServiceClient
	conn    *grpc.ClientConn
}

func NewClient(address string) (*Client, error) {
	if address == "" {
		address = DefaultAddress
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create Diodoros client: %w", err)
	}
	return &Client{service: v1.NewDiodorosServiceClient(conn), conn: conn}, nil
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
func (c *Client) CreateText(ctx context.Context, req *v1.CreateTextRequest, opts ...grpc.CallOption) (*v1.Text, error) {
	return c.service.CreateText(ctx, req, opts...)
}
func (c *Client) Options(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*v1.CorpusOptions, error) {
	return c.service.Options(ctx, req, opts...)
}
func (c *Client) CheckText(ctx context.Context, req *v1.CheckTextRequest, opts ...grpc.CallOption) (*v1.CheckTextResponse, error) {
	return c.service.CheckText(ctx, req, opts...)
}
