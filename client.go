package jmongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Client struct {
	client *mongo.Client
}

func NewClient(client *mongo.Client) *Client {
	return &Client{client: client}
}

// Database returns a handle for a database with the given name configured with the given DatabaseOptions.
func (c *Client) Database(name string, opts ...*options.DatabaseOptions) *Database {
	return NewDatabase(c.client.Database(name, opts...), c)
}

// WithTransaction open transaction
func (c *Client) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return c.client.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		_, err := sessionContext.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
			return nil, fn(sessCtx)
		})

		return err
	})
}

func WithTransaction[T any](ctx context.Context, c *Client, fn func(ctx context.Context) (T, error)) (T, error) {
	var res T
	var err error
	err = c.client.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		a, err := sessionContext.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
			return fn(sessCtx)
		})
		res = a.(T)
		return err
	})
	return res, err
}
