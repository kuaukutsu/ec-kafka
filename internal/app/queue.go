package app

import (
	"context"
	"fmt"
	"strings"

	"kafka/internal/config"
	"kafka/internal/queue"
	"kafka/pkg/kafka"
)

func (a *app) MakeProducer(ctx context.Context, cfg *config.Config) (*queue.Producer, error) {
	cl, err := kafka.New(
		ctx,
		kafka.SeedBrokers(strings.Split(cfg.Queue.Seed, ",")...),
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
	cl, err := kafka.New(
		ctx,
		kafka.SeedBrokers(strings.Split(cfg.Queue.Seed, ",")...),
		kafka.ConsumerDefault("consumer", "gokafka", "playground.events"),
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
