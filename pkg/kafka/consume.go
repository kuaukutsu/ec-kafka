package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"kafka/internal/queue"
)

const (
	// commitTimeout bounds the offset commit that runs after the consumer
	// context is cancelled; the commit must not hang the shutdown.
	commitTimeout = 5 * time.Second

	// handlerDrainTimeout is how long the handler of an in-flight record may
	// keep running after the consumer context is cancelled.
	handlerDrainTimeout = 5 * time.Second
)

// poller is the subset of *kgo.Client the consume loop needs.
type poller interface {
	PollFetches(ctx context.Context) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
}

// consume polls records, passes them to handle and commits processed offsets
// after every batch (at-least-once). It returns nil when ctx is cancelled.
func consume(ctx context.Context, p poller, handle queue.Handler, policy queue.FailurePolicy) error {
	for {
		stop, err := consumeOnce(ctx, p, handle, policy)
		if err != nil || stop {
			return err
		}
	}
}

// consumeOnce runs one poll cycle. stop reports that the loop must end.
func consumeOnce(
	ctx context.Context,
	p poller,
	handle queue.Handler,
	policy queue.FailurePolicy,
) (stop bool, err error) {
	fetches := p.PollFetches(ctx)
	// BlockRebalanceOnPoll is enabled: without this call a rebalance (and
	// client Close) would wait until the next poll.
	defer p.AllowRebalance()

	// Shutdown is not an error, so it is checked before the fetch errors:
	// a cancelled poll reports context.Canceled as a fetch error.
	if ctx.Err() != nil {
		return true, nil
	}

	if errs := fetchErrors(fetches); errs != nil {
		return true, fmt.Errorf("fetch: %w", errs)
	}

	processed, stopped, err := processBatch(ctx, fetches, handle, policy)

	if commitErr := commit(ctx, p, processed); commitErr != nil {
		err = errors.Join(err, commitErr)
	}

	return stopped || err != nil, err
}

// processBatch handles records in order and returns those that may be committed.
// stopped is true when the batch was cut short by shutdown or a policy error.
func processBatch(
	ctx context.Context,
	fetches kgo.Fetches,
	handle queue.Handler,
	policy queue.FailurePolicy,
) (processed []*kgo.Record, stopped bool, err error) {
	for iter := fetches.RecordIter(); !iter.Done(); {
		if ctx.Err() != nil {
			return processed, true, nil
		}

		rec := iter.Next()
		msg := toMessage(rec)

		hctx, release := handlerContext(ctx)
		handleErr := handle(hctx, msg)

		release()

		if handleErr != nil {
			// The handler most likely failed because of the shutdown:
			// the record was not handled, so it must be redelivered.
			if ctx.Err() != nil {
				return processed, true, nil
			}

			if policyErr := policy.OnFailure(ctx, msg, handleErr); policyErr != nil {
				// Shutdown during the policy's work (e.g. a future DLQ publish)
				// is not a failure: the record stays uncommitted and is redelivered.
				if ctx.Err() != nil {
					return processed, true, nil
				}

				return processed, true, fmt.Errorf("failure policy: %w", policyErr)
			}
		}

		processed = append(processed, rec)
	}

	return processed, false, nil
}

// handlerContext returns the context for one handler call. It is detached from
// ctx cancellation so the record in flight can finish, but only for
// handlerDrainTimeout after ctx is cancelled. The returned func must be called
// when the handler returns.
func handlerContext(ctx context.Context) (context.Context, context.CancelFunc) {
	hctx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	stop := context.AfterFunc(ctx, func() {
		time.AfterFunc(handlerDrainTimeout, cancel)
	})

	return hctx, func() {
		stop()
		cancel()
	}
}

// commit commits processed records with a context that survives shutdown.
func commit(ctx context.Context, p poller, processed []*kgo.Record) error {
	if len(processed) == 0 {
		return nil
	}

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()

	if err := p.CommitRecords(commitCtx, processed...); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func fetchErrors(fetches kgo.Fetches) error {
	var errs []error

	for _, fe := range fetches.Errors() {
		errs = append(errs, fe.Err)
	}

	return errors.Join(errs...)
}

func toMessage(rec *kgo.Record) queue.Message {
	return queue.Message{
		Topic:     rec.Topic,
		Key:       string(rec.Key),
		Value:     string(rec.Value),
		Partition: rec.Partition,
		Offset:    rec.Offset,
	}
}
