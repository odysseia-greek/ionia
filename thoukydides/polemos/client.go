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
func (c *Client) ListForms(ctx context.Context, req *v1.ListFormsRequest, opts ...grpc.CallOption) (*v1.ListFormsResponse, error) {
	return c.service.ListForms(ctx, req, opts...)
}
func (c *Client) GetForm(ctx context.Context, req *v1.GetFormRequest, opts ...grpc.CallOption) (*v1.Form, error) {
	return c.service.GetForm(ctx, req, opts...)
}
func (c *Client) StartReading(ctx context.Context, req *v1.StartReadingRequest, opts ...grpc.CallOption) (*v1.ReadingSession, error) {
	return c.service.StartReading(ctx, req, opts...)
}
func (c *Client) GetReading(ctx context.Context, req *v1.GetReadingRequest, opts ...grpc.CallOption) (*v1.ReadingSession, error) {
	return c.service.GetReading(ctx, req, opts...)
}
func (c *Client) SaveProgress(ctx context.Context, req *v1.SaveProgressRequest, opts ...grpc.CallOption) (*v1.ReadingSession, error) {
	return c.service.SaveProgress(ctx, req, opts...)
}
