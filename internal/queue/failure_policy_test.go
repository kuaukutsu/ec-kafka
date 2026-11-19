package queue

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

var errBoom = errors.New("boom")

func TestLogAndSkip_OnFailure(t *testing.T) {
	t.Parallel()

	type given struct {
		msg   Message
		cause error
	}

	type want struct {
		err      error
		contains []string
	}

	tests := map[string]struct {
		given given
		want  want
	}{
		"skips and logs record coordinates and cause": {
			given: given{
				msg:   Message{Topic: "t", Partition: 3, Offset: 42},
				cause: errBoom,
			},
			want: want{
				err:      nil,
				contains: []string{"topic=t", "partition=3", "offset=42", "boom"},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Given
			var buf bytes.Buffer
			policy := NewLogAndSkip(slog.New(slog.NewTextHandler(&buf, nil)))

			// When
			err := policy.OnFailure(context.Background(), tt.given.msg, tt.given.cause)

			// Then
			if !errors.Is(err, tt.want.err) {
				t.Fatalf("OnFailure() error = %v, want %v", err, tt.want.err)
			}

			for _, s := range tt.want.contains {
				if !strings.Contains(buf.String(), s) {
					t.Errorf("log %q does not contain %q", buf.String(), s)
				}
			}
		})
	}
}
