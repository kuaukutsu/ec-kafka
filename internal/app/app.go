package app

import (
	"context"
	"log/slog"

	"kafka/internal/config"
)

type app struct {
	log    *slog.Logger
	cfg    *config.Config
	closer *closer
}

func NewApp(ctx context.Context, log *slog.Logger, cfg *config.Config, name string) *app {
	c := NewCloser(log)

	return &app{
		log:    log,
		cfg:    cfg,
		closer: c,
	}
}

func (a *app) Close(ctx context.Context) {
	if err := a.closer.Close(ctx); err != nil {
		a.log.Error("app: shutdown error", slog.String("error", err.Error()))
	}
}
