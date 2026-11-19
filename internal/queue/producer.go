package queue

import "context"

type ProducerClient interface {
	ProduceSync(ctx context.Context, msg Message) error
}

type Producer struct {
	client ProducerClient
}

func NewProducer(client ProducerClient) *Producer {
	return &Producer{
		client: client,
	}
}

func (p *Producer) Push(ctx context.Context, msg Message) error {
	return p.client.ProduceSync(ctx, msg)
}
