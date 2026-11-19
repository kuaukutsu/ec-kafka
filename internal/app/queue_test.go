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

			// Given: a valid seed, so only the consumer settings can fail; validation
			// happens before any broker connection is attempted.
			cfg := &config.Config{Queue: config.Queue{Seed: "localhost:9092", Consumer: tt.given}}

			// When
			_, err := (&App{}).MakeConsumer(context.Background(), cfg)

			// Then
			if !errors.Is(err, tt.want) {
				t.Fatalf("MakeConsumer() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestApp_MakeProducer_RequiresSeed(t *testing.T) {
	t.Parallel()

	// Given: no seed brokers; validation happens before any connection attempt.
	cfg := &config.Config{}

	// When
	_, err := (&App{}).MakeProducer(context.Background(), cfg)

	// Then
	if !errors.Is(err, config.ErrSeedRequired) {
		t.Fatalf("MakeProducer() error = %v, want %v", err, config.ErrSeedRequired)
	}
}

func TestApp_MakeConsumer_RequiresSeed(t *testing.T) {
	t.Parallel()

	// Given: valid consumer settings, but no seed brokers.
	cfg := &config.Config{Queue: config.Queue{
		Consumer: config.Consumer{Group: "g", Topic: "t"},
	}}

	// When
	_, err := (&App{}).MakeConsumer(context.Background(), cfg)

	// Then
	if !errors.Is(err, config.ErrSeedRequired) {
		t.Fatalf("MakeConsumer() error = %v, want %v", err, config.ErrSeedRequired)
	}
}
