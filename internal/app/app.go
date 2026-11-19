package app

import (
	"context"
	"log/slog"
)

// App wires application dependencies and owns their shutdown.
type App struct {
	log    *slog.Logger
	closer *Closer
}

func NewApp(log *slog.Logger) *App {
	return &App{
		log:    log,
		closer: NewCloser(log),
	}
}

// Close runs all registered shutdown functions. It does not depend on ctx
// being alive: see Closer.Close.
func (a *App) Close(ctx context.Context) {
	if err := a.closer.Close(ctx); err != nil {
		a.log.Error("app: shutdown error", slog.String("error", err.Error()))
	}
}
