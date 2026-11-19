package queue

import (
	"context"
	"errors"
	"testing"
)

type fakeProducerClient struct {
	ctxVal any
	msg    Message
	calls  int
	err    error
}

func (f *fakeProducerClient) ProduceSync(ctx context.Context, msg Message) error {
	f.calls++
	f.ctxVal = ctx.Value(ctxKey{})
	f.msg = msg

	return f.err
}

func TestProducer_Push(t *testing.T) {
	t.Parallel()

	type given struct {
		msg       Message
		clientErr error
	}

	type want struct {
		err error
	}

	tests := map[string]struct {
		given given
		want  want
	}{
		"forwards ctx and message to the client": {
			given: given{msg: Message{Topic: "t", Key: "k", Value: "v"}},
			want:  want{err: nil},
		},
		"returns the client error": {
			given: given{msg: Message{Topic: "t"}, clientErr: errBoom},
			want:  want{err: errBoom},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Given
			fake := &fakeProducerClient{err: tt.given.clientErr}
			ctx := context.WithValue(context.Background(), ctxKey{}, "v")

			// When
			err := NewProducer(fake).Push(ctx, tt.given.msg)

			// Then
			if !errors.Is(err, tt.want.err) {
				t.Fatalf("Push() error = %v, want %v", err, tt.want.err)
			}

			if fake.calls != 1 {
				t.Errorf("ProduceSync calls = %d, want 1", fake.calls)
			}

			if fake.ctxVal != "v" {
				t.Error("client did not receive the same ctx")
			}

			if fake.msg != tt.given.msg {
				t.Errorf("client message = %+v, want %+v", fake.msg, tt.given.msg)
			}
		})
	}
}
