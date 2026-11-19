package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"kafka/internal/queue"
)

var (
	_ queue.ProducerClient = (*Client)(nil)
	_ queue.ConsumerClient = (*Client)(nil)
)

// Client is a Kafka client that implements both queue.ProducerClient and
// queue.ConsumerClient.
type Client struct {
	cl *kgo.Client
}

func New(ctx context.Context, opts ...Option) (*Client, error) {
	cfg := newConfig(opts...)

	cl, err := kgo.NewClient(cfg.kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("new client error: %w", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, cfg.pingTimeout)
	defer cancel()

	if err := cl.Ping(connectCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}

	return &Client{cl: cl}, nil
}

func (c *Client) ProduceSync(ctx context.Context, msg queue.Message) error {
	if err := c.cl.ProduceSync(ctx, newRecord(msg)).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	return nil
}

// newRecord converts a message to a kgo record. The key stays nil when the
// message has no key, so the client spreads such records over partitions
// instead of hashing an empty key into a single one.
func newRecord(msg queue.Message) *kgo.Record {
	return &kgo.Record{
		Topic: msg.GetTopic(),
		Key:   msg.GetKey(),
		Value: msg.GetValue(),
	}
}

// Consume blocks until ctx is cancelled (returns nil) or a non-retriable
// error occurs. Fetch errors that franz-go retries internally never reach it.
func (c *Client) Consume(ctx context.Context, handler queue.Handler, policy queue.FailurePolicy) error {
	return consume(ctx, c.cl, handler, policy)
}

func (c *Client) Close() {
	c.cl.Close()
}
