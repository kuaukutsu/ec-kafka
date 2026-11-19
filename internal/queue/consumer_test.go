package queue

import (
	"context"
	"errors"
	"testing"
)

type fakeQueueConsumer struct {
	ctxVal  any
	handler Handler
	policy  FailurePolicy
	err     error
}

func (f *fakeQueueConsumer) Consumer(ctx context.Context, handler Handler, policy FailurePolicy) error {
	f.ctxVal = ctx.Value(ctxKey{})
	f.handler = handler
	f.policy = policy

	return f.err
}

type ctxKey struct{}

type stubPolicy struct{}

func (stubPolicy) OnFailure(context.Context, Message, error) error { return nil }

func TestConsumer_Run(t *testing.T) {
	t.Parallel()

	type given struct {
		clientErr error
	}

	type want struct {
		err error
	}

	tests := map[string]struct {
		given given
		want  want
	}{
		"forwards ctx, handler and policy to the client": {
			given: given{clientErr: nil},
			want:  want{err: nil},
		},
		"returns the client error": {
			given: given{clientErr: errBoom},
			want:  want{err: errBoom},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Given
			fake := &fakeQueueConsumer{err: tt.given.clientErr}
			handled := false
			handler := func(context.Context, Message) error {
				handled = true
				return nil
			}
			policy := stubPolicy{}
			ctx := context.WithValue(context.Background(), ctxKey{}, "v")

			// When
			err := NewConsumer(fake, handler, policy).Run(ctx)

			// Then
			if !errors.Is(err, tt.want.err) {
				t.Fatalf("Run() error = %v, want %v", err, tt.want.err)
			}

			if fake.ctxVal != "v" {
				t.Error("client did not receive the same ctx")
			}

			if fake.policy != FailurePolicy(policy) {
				t.Error("client did not receive the same policy")
			}

			if err := fake.handler(ctx, Message{}); err != nil || !handled {
				t.Errorf("forwarded handler is not the given one (err=%v, handled=%v)", err, handled)
			}
		})
	}
}
