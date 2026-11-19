package queue

import "context"

// Handler processes a single consumed message.
type Handler func(ctx context.Context, msg Message) error

// FailurePolicy decides what happens to a message the Handler failed on.
type FailurePolicy interface {
	// OnFailure returns nil when the message is considered handled
	// (skipped, dead-lettered, ...) and its offset may be committed.
	// A non-nil error means the consumer must stop without committing the message.
	OnFailure(ctx context.Context, msg Message, cause error) error
}

type QueueConsumer interface {
	Consumer(ctx context.Context, handler Handler, policy FailurePolicy) error
}

type Consumer struct {
	client  QueueConsumer
	handler Handler
	policy  FailurePolicy
}

func NewConsumer(client QueueConsumer, handler Handler, policy FailurePolicy) *Consumer {
	return &Consumer{
		client:  client,
		handler: handler,
		policy:  policy,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	return c.client.Consumer(ctx, c.handler, c.policy)
}
