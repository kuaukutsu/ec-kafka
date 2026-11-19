package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const closerIterationTimeout = 10 * time.Second

type closeFn struct {
	name string
	fn   func(context.Context) error
}

type closer struct {
	log         *slog.Logger
	iterTimeout time.Duration // per-function budget; a field so tests can shorten it
	mu          sync.Mutex
	once        sync.Once
	funcs       []closeFn
	result      error // stored for repeated Close calls
}

func NewCloser(log *slog.Logger) *closer {
	return &closer{log: log, iterTimeout: closerIterationTimeout}
}

func (c *closer) Add(name string, fn func(context.Context) error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.funcs = append(c.funcs, closeFn{name: name, fn: fn})
}

// Close runs all registered functions in LIFO order.
// Each function gets its own closerIterationTimeout budget via a goroutine,
// so non-context-aware functions are also bounded by timeout.
// A function that outlives its timeout is abandoned, not awaited, so a hung
// function cannot block the shutdown; its late result is only logged.
// Errors (including timeouts) are collected and joined.
func (c *closer) Close(ctx context.Context) error {
	c.once.Do(func() {
		c.mu.Lock()
		funcs := c.funcs
		c.funcs = nil
		c.mu.Unlock()

		if len(funcs) == 0 {
			return
		}

		var errs []error

		for i := len(funcs) - 1; i >= 0; i-- {
			f := funcs[i]

			// context.WithoutCancel ensures each function gets its full
			// closerIterationTimeout even if the caller's ctx is already cancelled
			// (e.g. OS-signal context). The iteration timeout is the only bound.
			iterCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				c.iterTimeout,
			)

			done := make(chan error, 1)
			go func(fn func(context.Context) error) {
				done <- fn(iterCtx)
			}(f.fn)

			select {
			case err := <-done:
				cancel()
				if err != nil {
					errs = append(errs, fmt.Errorf("closer: %s: %w", f.name, err))
				}
			case <-iterCtx.Done():
				cancel()
				errs = append(errs, fmt.Errorf(
					"closer: %s: timed out after %s",
					f.name,
					c.iterTimeout,
				))
				// Do not wait for the function: it ignored its context, and
				// waiting would defeat the timeout. Report its late result
				// from a background goroutine instead.
				go func(name string, ch <-chan error) {
					if err := <-ch; err != nil {
						c.log.Warn("closer: late completion", slog.String("name", name), slog.String("error", err.Error()))
					} else {
						c.log.Warn("closer: late completion", slog.String("name", name))
					}
				}(f.name, done)
			}
		}

		c.result = errors.Join(errs...)
	})

	return c.result
}
