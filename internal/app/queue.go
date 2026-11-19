package app

import (
	"context"
	"fmt"

	"kafka/internal/config"
	"kafka/internal/queue"
	"kafka/pkg/kafka"
)

func (a *app) MakeProducer(ctx context.Context, cfg *config.Config) (*queue.Producer, error) {
	cl, err := kafka.New(
		ctx,
		kafka.SeedBrokers(cfg.Queue.Brokers()...),
		kafka.ProducerDefault("producer"),
		kafka.Timeout(cfg.Queue.Timeout),
		kafka.RecordDeliveryTimeout(cfg.Queue.DeliveryTimeout),
	)
	if err != nil {
		return nil, err
	}

	a.closer.Add("producer", func(ctx context.Context) error {
		cl.Close()
		return nil
	})

	return queue.NewProducer(cl), nil
}

func (a *app) MakeConsumer(ctx context.Context, cfg *config.Config) (*queue.Consumer, error) {
	if err := cfg.Queue.Consumer.Validate(); err != nil {
		return nil, fmt.Errorf("consumer config: %w", err)
	}

	cl, err := kafka.New(
		ctx,
		kafka.SeedBrokers(cfg.Queue.Brokers()...),
		kafka.ConsumerDefault("consumer", cfg.Queue.Consumer.Group, cfg.Queue.Consumer.Topic),
		kafka.Timeout(cfg.Queue.Timeout),
	)
	if err != nil {
		return nil, err
	}

	a.closer.Add("consumer", func(ctx context.Context) error {
		cl.Close()
		return nil
	})

	handler := func(_ context.Context, msg queue.Message) error {
		fmt.Println(msg.Value)

		return nil
	}

	return queue.NewConsumer(cl, handler, queue.NewLogAndSkip(a.log)), nil
}
