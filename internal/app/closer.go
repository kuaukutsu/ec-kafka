package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const closerIterationTimeout = 10 * time.Second

var ErrCloserClosed = errors.New("closer: already closing or closed")

type closeFn struct {
	name string
	fn   func(context.Context) error
}

// Closer is a LIFO registry of shutdown functions.
type Closer struct {
	iterTimeout time.Duration
	mu          sync.Mutex
	once        sync.Once
	closed      bool
	funcs       []closeFn
	result      error
}

func NewCloser() *Closer {
	return &Closer{iterTimeout: closerIterationTimeout}
}

// Add registers a function unless shutdown has already started.
// The caller retains responsibility for the resource if registration fails.
func (c *Closer) Add(name string, fn func(context.Context) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrCloserClosed
	}
	c.funcs = append(c.funcs, closeFn{name: name, fn: fn})
	return nil
}

// Close runs functions in LIFO order, respecting ctx and a per-function timeout.
// If a function times out, remaining functions are not run: the timed-out
// function may still be using their resources. Go cannot stop a function that
// ignores its context, so such a function may continue running after Close.
// Concurrent and subsequent calls wait for and return the first call's result.
func (c *Closer) Close(ctx context.Context) error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		funcs := c.funcs
		c.funcs = nil
		c.mu.Unlock()

		var errs []error
		for i := len(funcs) - 1; i >= 0; i-- {
			f := funcs[i]
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("closer: %s: not started: %w", f.name, err))
				break
			}

			err, interrupted := c.closeOne(ctx, f)
			if err != nil {
				errs = append(errs, fmt.Errorf("closer: %s: %w", f.name, err))
			}
			if interrupted {
				break
			}
		}
		c.result = errors.Join(errs...)
	})
	return c.result
}

// closeOne reports interruption when the function may still be running.
func (c *Closer) closeOne(ctx context.Context, f closeFn) (err error, interrupted bool) {
	iterCtx, cancel := context.WithTimeout(ctx, c.iterTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- f.fn(iterCtx) }()

	select {
	case err := <-done:
		return err, false
	case <-iterCtx.Done():
		return iterCtx.Err(), true
	}
}
