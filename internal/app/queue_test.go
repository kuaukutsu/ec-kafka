package app

import (
	"context"
	"errors"
	"testing"

	"kafka/internal/config"
)

func TestApp_MakeConsumer_RequiresGroupAndTopic(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given config.Consumer
		want  error
	}{
		"group missing": {given: config.Consumer{Topic: "t"}, want: config.ErrConsumerGroupRequired},
		"topic missing": {given: config.Consumer{Group: "g"}, want: config.ErrConsumerTopicRequired},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Given: validation happens before any broker connection is attempted.
			cfg := &config.Config{Queue: config.Queue{Consumer: tt.given}}

			// When
			_, err := (&app{}).MakeConsumer(context.Background(), cfg)

			// Then
			if !errors.Is(err, tt.want) {
				t.Fatalf("MakeConsumer() error = %v, want %v", err, tt.want)
			}
		})
	}
}
