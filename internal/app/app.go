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
		closer: NewCloser(),
	}
}

// Close runs shutdown functions even if the caller's context is cancelled.
// Context values are preserved, but cancellation and deadlines are not.
func (a *App) Close(ctx context.Context) {
	if err := a.closer.Close(context.WithoutCancel(ctx)); err != nil {
		a.log.Error("app: shutdown error", slog.String("error", err.Error()))
	}
}
