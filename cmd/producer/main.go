package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"kafka/internal/app"
	"kafka/internal/config"
	"kafka/internal/queue"
)

func main() {
	cfg, err := config.LoadFromArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}

	log := app.NewLogger(cfg.Env).With(
		slog.String("service", "producer"),
	)

	log.Debug("producer: starting", slog.Any("config", cfg))

	if err := run(log, cfg); err != nil {
		log.Error("producer: fatal", slog.Any("err", err))
		os.Exit(1)
	}

	// run has returned, so all deferred shutdown steps (closer) are done.
	log.Info("producer: shutdown complete")
}

func run(log *slog.Logger, cfg *config.Config) error {
	ctx, stop := app.SignalContext(context.Background())
	defer stop()

	application := app.NewApp(log)
	// Closer ignores cancellation of ctx, so shutdown still runs after a signal.
	defer application.Close(ctx)

	p, err := application.MakeProducer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("producer make: %w", err)
	}

	pushCtx, cancel := context.WithTimeout(ctx, cfg.Queue.DeliveryTimeout)
	defer cancel()

	for i := range 10 {
		msg := queue.Message{
			Topic: "playground.events",
			Key:   "key" + strconv.Itoa(i%2),
			Value: fmt.Sprintf("event-%d", i),
		}

		if err := p.Push(pushCtx, msg); err != nil {
			return fmt.Errorf("producer push: %w", err)
		}

		log.Info("producer: message sent", slog.String("topic", msg.Topic))
	}

	return nil
}
