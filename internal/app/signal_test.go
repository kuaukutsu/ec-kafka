package app

import (
	"context"
	"syscall"
	"testing"
	"time"
)

// Not parallel: the test delivers a process-wide signal.
func TestSignalContext_CancelledOnSIGTERM(t *testing.T) {
	// Given
	ctx, stop := SignalContext(context.Background())
	defer stop()

	// When: only one signal is sent; a second one would use the default
	// behavior and kill the test process.
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send signal: %v", err)
	}

	// Then
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("context was not cancelled after SIGTERM")
	}
}

func TestSignalContext_StopCancelsContext(t *testing.T) {
	t.Parallel()

	ctx, stop := SignalContext(context.Background())

	stop()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not cancelled by stop")
	}
}
