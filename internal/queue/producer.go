package queue

import "context"

type QueueProducer interface {
	ProduceSync(ctx context.Context, msg Message) error
}

type Producer struct {
	client QueueProducer
}

func NewProducer(client QueueProducer) *Producer {
	return &Producer{
		client: client,
	}
}

func (p *Producer) Push(ctx context.Context, msg Message) error {
	return p.client.ProduceSync(ctx, msg)
}
