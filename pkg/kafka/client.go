package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"kafka/internal/queue"
)

var (
	_ queue.QueueProducer = (*client)(nil)
	_ queue.QueueConsumer = (*client)(nil)
)

type client struct {
	cl *kgo.Client
}

func New(ctx context.Context, opts ...Option) (*client, error) {
	cfg := newConfig(opts...)

	cl, err := kgo.NewClient(cfg.kgoOpts...)
	if err != nil {
		return nil, fmt.Errorf("new client error: %w", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := cl.Ping(connectCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}

	return &client{cl: cl}, nil
}

func (c *client) ProduceSync(ctx context.Context, msg queue.Message) error {
	record := &kgo.Record{
		Topic: msg.Topic,
		Key:   []byte(msg.Key),
		Value: []byte(msg.Value),
	}

	// Alternatively, ProduceSync exists to synchronously produce a batch of records.
	if err := c.cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("record had a produce error while synchronously producing: %w", err)
	}

	return nil
}

// Consumer blocks until ctx is cancelled (returns nil) or a non-retriable
// error occurs. Fetch errors that franz-go retries internally never reach it.
func (c *client) Consumer(ctx context.Context, handler queue.Handler, policy queue.FailurePolicy) error {
	return consume(ctx, c.cl, handler, policy)
}

func (c *client) Close() {
	c.cl.Close()
}
