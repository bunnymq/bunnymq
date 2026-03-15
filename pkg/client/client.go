package client

import (
	"context"
	"errors"
)

// Consumer reads messages from BunnyMQ topics.
type Consumer struct{}

// GroupConsumer participates in a consumer group with automatic partition assignment.
type GroupConsumer struct{}

// Fetch reads up to maxBytes from the given topic/partition starting at offset.
func (c *Consumer) Fetch(ctx context.Context, topic string, partitionID int32, offset int64, maxBytes int) ([]byte, int64, error) {
	return nil, 0, errors.New("not implemented")
}
