package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"kafka/internal/app"
	"kafka/internal/config"
)

func main() {
	cfg, err := config.LoadFromArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}

	log := app.NewLogger(cfg.Env).With(
		slog.String("service", "consumer"),
	)

	log.Debug("consumer: starting", slog.Any("config", cfg))

	if err := run(log, cfg); err != nil {
		log.Error("consumer: fatal", slog.Any("err", err))
		os.Exit(1)
	}

	// run has returned, so all deferred shutdown steps (closer) are done.
	log.Info("consumer: shutdown complete")
}

func run(log *slog.Logger, cfg *config.Config) error {
	ctx, stop := app.SignalContext(context.Background())
	defer stop()

	application := app.NewApp(log)
	defer application.Close(ctx)

	consumer, err := application.MakeConsumer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("consumer make: %w", err)
	}

	log.Info("consumer: ready")

	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer run: %w", err)
	}

	return nil
}
