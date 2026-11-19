package kafka

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"kafka/internal/queue"
)

var errBoom = errors.New("boom")

const testTopic = "t"

type commitCall struct {
	offsets     []int64
	ctxErr      error
	hasDeadline bool
	deadline    time.Time
}

// fakePoller replays scripted batches of record offsets (one partition).
// When the script is exhausted it returns fetchErr (if set) or cancels the
// consumer context, like a real client reacting to a shutdown signal.
type fakePoller struct {
	cancel    context.CancelFunc
	batches   [][]int64
	fetches   []kgo.Fetches // served before batches; for multi-partition scenarios
	nilKV     bool
	fetchErr  error
	commitErr error

	polls   int
	allow   int
	events  []string
	commits []commitCall
}

func (f *fakePoller) PollFetches(ctx context.Context) kgo.Fetches {
	f.polls++

	if ctx.Err() != nil {
		return f.errFetches(ctx.Err())
	}

	if len(f.fetches) > 0 {
		next := f.fetches[0]
		f.fetches = f.fetches[1:]

		return next
	}

	if len(f.batches) == 0 {
		if f.fetchErr != nil {
			return f.errFetches(f.fetchErr)
		}

		f.cancel()

		return f.errFetches(ctx.Err())
	}

	offsets := f.batches[0]
	f.batches = f.batches[1:]

	records := make([]*kgo.Record, 0, len(offsets))
	for _, o := range offsets {
		r := &kgo.Record{Topic: testTopic, Partition: 0, Offset: o}
		if !f.nilKV {
			r.Key, r.Value = []byte("k"), []byte("v")
		}

		records = append(records, r)
	}

	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      testTopic,
		Partitions: []kgo.FetchPartition{{Partition: 0, Records: records}},
	}}}}
}

func (f *fakePoller) errFetches(err error) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      testTopic,
		Partitions: []kgo.FetchPartition{{Partition: -1, Err: err}},
	}}}}
}

func (f *fakePoller) CommitRecords(ctx context.Context, rs ...*kgo.Record) error {
	call := commitCall{ctxErr: ctx.Err()}
	call.deadline, call.hasDeadline = ctx.Deadline()

	for _, r := range rs {
		call.offsets = append(call.offsets, r.Offset)
	}

	f.commits = append(f.commits, call)
	f.events = append(f.events, "commit")

	return f.commitErr
}

func (f *fakePoller) AllowRebalance() {
	f.allow++
	f.events = append(f.events, "allow")
}

type recordingPolicy struct {
	cancel  context.CancelFunc // when set, called inside OnFailure (shutdown during policy work)
	err     error
	offsets []int64
	causes  []error
	msgs    []queue.Message
}

func (p *recordingPolicy) OnFailure(_ context.Context, msg queue.Message, cause error) error {
	p.offsets = append(p.offsets, msg.Offset)
	p.causes = append(p.causes, cause)
	p.msgs = append(p.msgs, msg)

	if p.cancel != nil {
		p.cancel()
	}

	return p.err
}

func TestConsume(t *testing.T) {
	t.Parallel()

	type given struct {
		batches     [][]int64
		handlerErrs map[int64]error // offset -> error returned by the handler
		cancelAt    int64           // handler cancels ctx on this offset; 0 = never
		policyErr   error
		policyStops bool // policy cancels ctx and fails (shutdown while it works)
		commitErr   error
		fetchErr    error
		nilKV       bool
	}

	type want struct {
		err       error // nil: Run must return nil
		handled   []int64
		committed [][]int64
		policy    []int64
	}

	tests := map[string]struct {
		given given
		want  want
	}{
		"shutdown with empty script returns nil without commit": {
			given: given{},
			want:  want{},
		},
		"batch processed, committed once after the batch": {
			given: given{batches: [][]int64{{1, 2, 3}}},
			want: want{
				handled:   []int64{1, 2, 3},
				committed: [][]int64{{1, 2, 3}},
			},
		},
		"handler error, policy skips, record is committed": {
			given: given{
				batches:     [][]int64{{1, 2, 3}},
				handlerErrs: map[int64]error{2: errBoom},
			},
			want: want{
				handled:   []int64{1, 2, 3},
				committed: [][]int64{{1, 2, 3}},
				policy:    []int64{2},
			},
		},
		"handler deadline exceeded with live ctx goes to policy": {
			given: given{
				batches:     [][]int64{{1}},
				handlerErrs: map[int64]error{1: context.DeadlineExceeded},
			},
			want: want{
				handled:   []int64{1},
				committed: [][]int64{{1}},
				policy:    []int64{1},
			},
		},
		"handler fails because ctx was cancelled: not committed, policy untouched": {
			given: given{
				batches:     [][]int64{{1, 2, 3}},
				handlerErrs: map[int64]error{2: context.Canceled},
				cancelAt:    2,
			},
			want: want{
				handled:   []int64{1, 2},
				committed: [][]int64{{1}},
			},
		},
		"ctx cancelled between records: rest of the batch is not handled": {
			given: given{
				batches:  [][]int64{{1, 2, 3}},
				cancelAt: 1,
			},
			want: want{
				handled:   []int64{1},
				committed: [][]int64{{1}},
			},
		},
		"policy error stops consumer, failed record not committed": {
			given: given{
				batches:     [][]int64{{1, 2, 3}},
				handlerErrs: map[int64]error{2: errBoom},
				policyErr:   errBoom,
			},
			want: want{
				err:       errBoom,
				handled:   []int64{1, 2},
				committed: [][]int64{{1}},
				policy:    []int64{2},
			},
		},
		"policy fails because of shutdown: returns nil, record not committed": {
			given: given{
				batches:     [][]int64{{1, 2, 3}},
				handlerErrs: map[int64]error{2: errBoom},
				policyErr:   context.Canceled,
				policyStops: true,
			},
			want: want{
				handled:   []int64{1, 2},
				committed: [][]int64{{1}},
				policy:    []int64{2},
			},
		},
		"fetch error with live ctx is returned": {
			given: given{fetchErr: kgo.ErrClientClosed},
			want:  want{err: kgo.ErrClientClosed},
		},
		"commit error is returned": {
			given: given{batches: [][]int64{{1}}, commitErr: errBoom},
			want: want{
				err:       errBoom,
				handled:   []int64{1},
				committed: [][]int64{{1}},
			},
		},
		"empty batch does not commit": {
			given: given{batches: [][]int64{{}}},
			want:  want{},
		},
		"nil key and value become empty strings": {
			given: given{batches: [][]int64{{1}}, nilKV: true},
			want: want{
				handled:   []int64{1},
				committed: [][]int64{{1}},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Given
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			poller := &fakePoller{
				cancel:    cancel,
				batches:   tt.given.batches,
				nilKV:     tt.given.nilKV,
				fetchErr:  tt.given.fetchErr,
				commitErr: tt.given.commitErr,
			}
			policy := &recordingPolicy{err: tt.given.policyErr}
			if tt.given.policyStops {
				policy.cancel = cancel
			}

			var handled []int64

			var msgs []queue.Message

			handler := func(_ context.Context, msg queue.Message) error {
				handled = append(handled, msg.Offset)
				msgs = append(msgs, msg)

				if msg.Offset == tt.given.cancelAt {
					cancel()
				}

				return tt.given.handlerErrs[msg.Offset]
			}

			// When
			err := consume(ctx, poller, handler, policy)

			// Then
			if !errors.Is(err, tt.want.err) {
				t.Fatalf("consume() error = %v, want %v", err, tt.want.err)
			}

			if !slices.Equal(handled, tt.want.handled) {
				t.Errorf("handled = %v, want %v", handled, tt.want.handled)
			}

			if !slices.Equal(policy.offsets, tt.want.policy) {
				t.Errorf("policy offsets = %v, want %v", policy.offsets, tt.want.policy)
			}

			assertCommits(t, poller, tt.want.committed)

			if poller.allow != poller.polls {
				t.Errorf("AllowRebalance calls = %d, polls = %d, want equal", poller.allow, poller.polls)
			}

			for _, m := range msgs {
				if tt.given.nilKV && (m.Key != "" || m.Value != "") {
					t.Errorf("message = %+v, want empty key and value", m)
				}

				if m.Topic != testTopic {
					t.Errorf("message topic = %q, want %q", m.Topic, testTopic)
				}
			}

			for i, e := range poller.events {
				if e == "commit" && (i+1 >= len(poller.events) || poller.events[i+1] != "allow") {
					t.Errorf("events = %v: commit at %d is not followed by allow", poller.events, i)
				}
			}
		})
	}
}

func assertCommits(t *testing.T, p *fakePoller, want [][]int64) {
	t.Helper()

	if len(p.commits) != len(want) {
		t.Fatalf("commit calls = %d, want %d (%v)", len(p.commits), len(want), p.commits)
	}

	for i, c := range p.commits {
		if !slices.Equal(c.offsets, want[i]) {
			t.Errorf("commit[%d] offsets = %v, want %v", i, c.offsets, want[i])
		}

		if c.ctxErr != nil {
			t.Errorf("commit[%d] ctx is cancelled: %v", i, c.ctxErr)
		}

		if !c.hasDeadline || time.Until(c.deadline) > commitTimeout {
			t.Errorf("commit[%d] ctx deadline must be within %s", i, commitTimeout)
		}
	}
}

func TestConsume_PolicyReceivesRecordCoordinatesAndCause(t *testing.T) {
	t.Parallel()

	// Given
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := &fakePoller{cancel: cancel, batches: [][]int64{{7}}}
	policy := &recordingPolicy{}
	handler := func(context.Context, queue.Message) error { return errBoom }

	// When
	err := consume(ctx, poller, handler, policy)
	// Then
	if err != nil {
		t.Fatalf("consume() error = %v, want nil", err)
	}

	if len(policy.msgs) != 1 {
		t.Fatalf("policy calls = %d, want 1", len(policy.msgs))
	}

	got := policy.msgs[0]
	if got.Topic != testTopic || got.Partition != 0 || got.Offset != 7 {
		t.Errorf("policy msg = %+v, want topic=%q partition=0 offset=7", got, testTopic)
	}

	if !errors.Is(policy.causes[0], errBoom) {
		t.Errorf("policy cause = %v, want %v", policy.causes[0], errBoom)
	}
}

func partitionFetch(partition int32, offsets ...int64) kgo.FetchPartition {
	records := make([]*kgo.Record, 0, len(offsets))
	for _, o := range offsets {
		records = append(records, &kgo.Record{Topic: testTopic, Partition: partition, Offset: o})
	}

	return kgo.FetchPartition{Partition: partition, Records: records}
}

func TestConsume_MultiPartitionBatchCutMidway(t *testing.T) {
	t.Parallel()

	type given struct {
		cancelAt    int64
		handlerErrs map[int64]error
		policyErr   error
	}

	type want struct {
		err       error
		handled   []int64
		committed []int64
	}

	tests := map[string]struct {
		given given
		want  want
	}{
		"shutdown inside the second partition commits the prefix of both": {
			given: given{cancelAt: 21},
			want: want{
				handled:   []int64{10, 11, 20, 21},
				committed: []int64{10, 11, 20, 21},
			},
		},
		"policy error inside the second partition commits only what precedes it": {
			given: given{handlerErrs: map[int64]error{21: errBoom}, policyErr: errBoom},
			want: want{
				err:       errBoom,
				handled:   []int64{10, 11, 20, 21},
				committed: []int64{10, 11, 20},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Given
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			poller := &fakePoller{cancel: cancel, fetches: []kgo.Fetches{{{Topics: []kgo.FetchTopic{{
				Topic: testTopic,
				Partitions: []kgo.FetchPartition{
					partitionFetch(0, 10, 11),
					partitionFetch(1, 20, 21, 22),
				},
			}}}}}}

			var handled []int64

			handler := func(_ context.Context, msg queue.Message) error {
				handled = append(handled, msg.Offset)

				if msg.Offset == tt.given.cancelAt {
					cancel()
				}

				return tt.given.handlerErrs[msg.Offset]
			}

			// When
			err := consume(ctx, poller, handler, &recordingPolicy{err: tt.given.policyErr})

			// Then
			if !errors.Is(err, tt.want.err) {
				t.Fatalf("consume() error = %v, want %v", err, tt.want.err)
			}

			if !slices.Equal(handled, tt.want.handled) {
				t.Errorf("handled = %v, want %v", handled, tt.want.handled)
			}

			assertCommits(t, poller, [][]int64{tt.want.committed})
		})
	}
}

func TestConsume_HandlerCtxSurvivesShutdownUntilDrainTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Given
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		poller := &fakePoller{cancel: cancel, batches: [][]int64{{1, 2}}}

		var (
			handled        []int64
			aliveAfterStop bool
			waited         time.Duration
		)

		handler := func(hctx context.Context, msg queue.Message) error {
			handled = append(handled, msg.Offset)

			cancel() // shutdown signal arrives while the record is in flight

			aliveAfterStop = hctx.Err() == nil

			start := time.Now()
			<-hctx.Done() // a handler that never finishes on its own

			waited = time.Since(start)

			return hctx.Err()
		}

		// When
		err := consume(ctx, poller, handler, &recordingPolicy{})
		// Then
		if err != nil {
			t.Fatalf("consume() error = %v, want nil", err)
		}

		if !aliveAfterStop {
			t.Error("handler ctx must stay alive right after the shutdown signal")
		}

		if waited != handlerDrainTimeout {
			t.Errorf("handler ctx cancelled after %s, want %s", waited, handlerDrainTimeout)
		}

		if !slices.Equal(handled, []int64{1}) {
			t.Errorf("handled = %v, want [1]", handled)
		}

		assertCommits(t, poller, nil) // the interrupted record is not committed
	})
}
