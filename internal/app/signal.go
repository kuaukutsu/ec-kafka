package app

import (
	"context"
	"os/signal"
	"syscall"
)

// SignalContext returns a context cancelled on the first SIGINT/SIGTERM.
// The default signal behavior is restored right after that, so a second
// signal terminates the process if the shutdown gets stuck.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-ctx.Done()
		stop()
	}()

	return ctx, stop
}
