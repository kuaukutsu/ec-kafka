package main

import (
	"context"
	"log/slog"
	"os"
	"time"

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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	application := app.NewApp(ctx, log, cfg, "console")
	defer application.Close(ctx)

	p, err := application.MakeProducer(ctx, cfg)
	if err != nil {
		log.Error("producer: fatal", slog.Any("err", err))
		os.Exit(1)
	}

	p.Push(context.Background(), queue.Message{
		Topic: "playground.events",
		Value: "test",
	})
}
