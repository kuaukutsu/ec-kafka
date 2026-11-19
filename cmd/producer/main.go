package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"kafka/internal/app"
	"kafka/internal/config"
	"kafka/internal/queue"
)

func main() {
	cfg := config.New()
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Restore default signal behavior after the first signal so that a second
	// SIGINT/SIGTERM terminates the process during a stuck shutdown.
	go func() {
		<-ctx.Done()
		stop()
	}()

	application := app.NewApp(ctx, log, cfg, "producer")
	// Shutdown must not depend on the (possibly cancelled) run context.
	defer application.Close(context.WithoutCancel(ctx))

	p, err := application.MakeProducer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("producer make: %w", err)
	}

	pushCtx, cancel := context.WithTimeout(ctx, cfg.Queue.DeliveryTimeout)
	defer cancel()

	msg := queue.Message{
		Topic: "playground.events",
		Value: "test",
	}

	if err := p.Push(pushCtx, msg); err != nil {
		return fmt.Errorf("producer push: %w", err)
	}

	log.Info("producer: message sent", slog.String("topic", msg.Topic))

	return nil
}
