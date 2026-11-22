package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

func TestAppCloseAfterCancellation(t *testing.T) {
	t.Parallel()
	a := NewApp(slog.New(slog.NewTextHandler(io.Discard, nil)))
	type contextKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
	cancel()

	var called bool
	var gotErr error
	var gotValue any
	if err := a.closer.Add("check", func(ctx context.Context) error {
		called = true
		gotErr = ctx.Err()
		gotValue = ctx.Value(contextKey{})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	a.Close(ctx)
	if !called || gotErr != nil || gotValue != "value" {
		t.Fatalf("shutdown context: called = %v, err = %v, value = %v", called, gotErr, gotValue)
	}
}
