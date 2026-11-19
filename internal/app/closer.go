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
	log    *slog.Logger
	mu     sync.Mutex
	once   sync.Once
	funcs  []closeFn
	result error // stored for repeated Close calls
}

func NewCloser(log *slog.Logger) *closer {
	return &closer{log: log}
}

func (c *closer) Add(name string, fn func(context.Context) error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.funcs = append(c.funcs, closeFn{name: name, fn: fn})
}

// Close runs all registered functions in LIFO order.
// Each function gets its own closerIterationTimeout budget via a goroutine,
// so non-context-aware functions are also bounded by timeout.
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
		var lateWg sync.WaitGroup

		for i := len(funcs) - 1; i >= 0; i-- {
			f := funcs[i]

			// context.WithoutCancel ensures each function gets its full
			// closerIterationTimeout even if the caller's ctx is already cancelled
			// (e.g. OS-signal context). The iteration timeout is the only bound.
			iterCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				closerIterationTimeout,
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
					closerIterationTimeout,
				))
				lateWg.Add(1)
				go func(name string, ch <-chan error) {
					defer lateWg.Done()
					if err := <-ch; err != nil {
						c.log.Warn("closer: late completion", slog.String("name", name), slog.String("error", err.Error()))
					} else {
						c.log.Warn("closer: late completion", slog.String("name", name))
					}
				}(f.name, done)
			}
		}

		lateWg.Wait()
		c.result = errors.Join(errs...)
	})

	return c.result
}
