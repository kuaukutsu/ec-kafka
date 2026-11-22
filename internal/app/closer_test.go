package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testIterTimeout = 100 * time.Millisecond

func newTestCloser() *Closer {
	c := NewCloser()
	c.iterTimeout = testIterTimeout
	return c
}

func addTestClose(t *testing.T, c *Closer, name string, fn func(context.Context) error) {
	t.Helper()
	if err := c.Add(name, fn); err != nil {
		t.Fatalf("Add(%q): %v", name, err)
	}
}

func TestCloserRunsInLIFOOrderAndJoinsErrors(t *testing.T) {
	t.Parallel()
	c := newTestCloser()
	errA, errB := errors.New("a"), errors.New("b")
	var order []string
	for _, item := range []struct {
		name string
		err  error
	}{{"a", errA}, {"ok", nil}, {"b", errB}} {
		addTestClose(t, c, item.name, func(context.Context) error {
			order = append(order, item.name)
			return item.err
		})
	}
	err := c.Close(context.Background())
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("Close() = %v, want both errors", err)
	}
	if len(order) != 3 || order[0] != "b" || order[1] != "ok" || order[2] != "a" {
		t.Fatalf("order = %v, want [b ok a]", order)
	}
}

func TestCloserConcurrentCloseAndLateAdd(t *testing.T) {
	t.Parallel()
	c := newTestCloser()
	started, release := make(chan struct{}), make(chan struct{})
	errBoom := errors.New("boom")
	var calls atomic.Int32
	addTestClose(t, c, "blocking", func(context.Context) error {
		calls.Add(1)
		close(started)
		<-release
		return errBoom
	})
	const n = 8
	results := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() { results <- c.Close(context.Background()) })
	}
	<-started
	if err := c.Add("late", func(context.Context) error { return nil }); !errors.Is(err, ErrCloserClosed) {
		t.Fatalf("Add during Close = %v", err)
	}
	close(release)
	wg.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, errBoom) {
			t.Fatalf("Close = %v, want boom", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
	if err := c.Add("later", func(context.Context) error { return nil }); !errors.Is(err, ErrCloserClosed) {
		t.Fatalf("Add after Close = %v", err)
	}
}

func TestCloserTimeoutDoesNotStartDependencies(t *testing.T) {
	t.Parallel()
	c := newTestCloser()
	release, exited := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release); <-exited })
	var olderRan atomic.Bool
	addTestClose(t, c, "older", func(context.Context) error { olderRan.Store(true); return nil })
	addTestClose(t, c, "hung", func(context.Context) error { defer close(exited); <-release; return nil })
	err := c.Close(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want deadline", err)
	}
	if olderRan.Load() {
		t.Fatal("dependency closed while callback is running")
	}
}

func TestCloserRespectsContext(t *testing.T) {
	t.Parallel()
	t.Run("already cancelled", func(t *testing.T) {
		c := newTestCloser()
		var ran atomic.Bool
		addTestClose(t, c, "fn", func(context.Context) error { ran.Store(true); return nil })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Close = %v", err)
		}
		if ran.Load() {
			t.Fatal("callback started after cancellation")
		}
	})
	t.Run("cancel during callback", func(t *testing.T) {
		c := newTestCloser()
		started, exited := make(chan struct{}), make(chan struct{})
		addTestClose(t, c, "fn", func(ctx context.Context) error {
			defer close(exited)
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- c.Close(ctx) }()
		<-started
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Close = %v", err)
		}
		<-exited
	})
}

func TestCloserOverallDeadline(t *testing.T) {
	t.Parallel()
	c := newTestCloser()
	c.iterTimeout = time.Second
	started, exited := make(chan struct{}), make(chan struct{})
	addTestClose(t, c, "fn", func(ctx context.Context) error {
		defer close(exited)
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), testIterTimeout)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.Close(ctx) }()
	<-started
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want overall deadline", err)
	}
	<-exited
}

func TestCloserContinuesAfterCallbackReturnsDeadlineExceeded(t *testing.T) {
	t.Parallel()
	c := newTestCloser()
	var olderRan atomic.Bool
	addTestClose(t, c, "older", func(context.Context) error {
		olderRan.Store(true)
		return nil
	})
	addTestClose(t, c, "error", func(context.Context) error {
		return context.DeadlineExceeded
	})

	err := c.Close(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want deadline exceeded", err)
	}
	if !olderRan.Load() {
		t.Fatal("a completed callback's deadline error prevented the next callback")
	}
}
