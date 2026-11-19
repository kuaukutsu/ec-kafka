package queue

import (
	"context"
	"log/slog"
)

var _ FailurePolicy = LogAndSkip{}

// LogAndSkip logs the failed message and skips it.
type LogAndSkip struct {
	log *slog.Logger
}

func NewLogAndSkip(log *slog.Logger) LogAndSkip {
	return LogAndSkip{log: log}
}

func (p LogAndSkip) OnFailure(ctx context.Context, msg Message, cause error) error {
	p.log.ErrorContext(ctx, "queue: handler failed, message skipped",
		slog.String("topic", msg.Topic),
		slog.Int("partition", int(msg.Partition)),
		slog.Int64("offset", msg.Offset),
		slog.Any("err", cause),
	)

	return nil
}
