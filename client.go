package jmongo

import (
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
    return  NewDatabase(c.client.Database(name, opts...))
}