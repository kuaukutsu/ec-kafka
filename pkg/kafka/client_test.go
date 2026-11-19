package kafka

import (
	"testing"

	"kafka/internal/queue"
)

func TestNewRecord_Key(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given   string
		wantNil bool
		want    string
	}{
		// A non-nil empty key would be hashed by franz-go and pin every
		// keyless message to one partition.
		"empty key stays nil": {given: "", wantNil: true},
		"key is kept":         {given: "order-1", want: "order-1"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Given
			msg := queue.Message{Topic: "t", Key: tt.given, Value: "v"}

			// When
			rec := newRecord(msg)

			// Then
			if tt.wantNil && rec.Key != nil {
				t.Fatalf("Key = %#v, want nil", rec.Key)
			}

			if !tt.wantNil && string(rec.Key) != tt.want {
				t.Fatalf("Key = %q, want %q", rec.Key, tt.want)
			}

			if rec.Topic != "t" || string(rec.Value) != "v" {
				t.Fatalf("record = topic %q value %q, want t / v", rec.Topic, rec.Value)
			}
		})
	}
}
