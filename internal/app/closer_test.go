package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

const testIterTimeout = 50 * time.Millisecond

func newTestCloser() *Closer {
	c := NewCloser(slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.iterTimeout = testIterTimeout

	return c
}

func TestCloser_RunsInLIFOOrder(t *testing.T) {
	t.Parallel()

	// Given
	c := newTestCloser()

	var order []string

	for _, name := range []string{"first", "second", "third"} {
		c.Add(name, func(context.Context) error {
			order = append(order, name)

			return nil
		})
	}

	// When
	err := c.Close(context.Background())

	// Then
	if err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}

	want := []string{"third", "second", "first"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}

	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestCloser_CloseIsIdempotent(t *testing.T) {
	t.Parallel()

	// Given
	c := newTestCloser()
	errBoom := errors.New("boom")

	var calls atomic.Int32

	c.Add("failing", func(context.Context) error {
		calls.Add(1)

		return errBoom
	})

	// When
	first := c.Close(context.Background())
	second := c.Close(context.Background())

	// Then
	if !errors.Is(first, errBoom) || !errors.Is(second, errBoom) {
		t.Fatalf("Close() errors = %v, %v, want both to wrap %v", first, second, errBoom)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("function called %d times, want 1", got)
	}
}

func TestCloser_JoinsErrorsAndContinuesAfterFailure(t *testing.T) {
	t.Parallel()

	// Given
	c := newTestCloser()
	errA, errB := errors.New("a failed"), errors.New("b failed")

	var okRan atomic.Bool

	c.Add("a", func(context.Context) error { return errA })
	c.Add("ok", func(context.Context) error { okRan.Store(true); return nil })
	c.Add("b", func(context.Context) error { return errB })

	// When
	err := c.Close(context.Background())

	// Then
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("Close() error = %v, want it to wrap both %v and %v", err, errA, errB)
	}

	if !okRan.Load() {
		t.Fatal("a function after a failing one was not run")
	}
}

func TestCloser_WorksAfterParentContextCancelled(t *testing.T) {
	t.Parallel()

	// Given: the parent context is already cancelled (e.g. after SIGTERM).
	c := newTestCloser()

	var ctxErr error

	c.Add("fn", func(ctx context.Context) error {
		ctxErr = ctx.Err()

		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// When
	err := c.Close(ctx)

	// Then
	if err != nil || ctxErr != nil {
		t.Fatalf("Close() error = %v, fn ctx error = %v, want both nil", err, ctxErr)
	}
}

// A function that ignores its context and never returns must not hang Close:
// Close has to return once the per-function timeout has passed.
func TestCloser_HungFunctionDoesNotBlockClose(t *testing.T) {
	t.Parallel()

	// Given
	c := newTestCloser()
	hang := make(chan struct{})

	t.Cleanup(func() { close(hang) }) // release the leaked goroutine after the test

	var olderRan atomic.Bool

	c.Add("older", func(context.Context) error { olderRan.Store(true); return nil })
	c.Add("hung", func(context.Context) error { <-hang; return nil })

	// When
	done := make(chan error, 1)

	go func() { done <- c.Close(context.Background()) }()

	// Then
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Close() error = nil, want a timeout error for the hung function")
		}

		if !olderRan.Load() {
			t.Fatal("function registered before the hung one was not run")
		}
	case <-time.After(20 * testIterTimeout):
		t.Fatal("Close() did not return: a hung function blocks shutdown")
	}
}
