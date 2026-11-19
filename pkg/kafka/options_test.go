package kafka

import (
	"testing"
	"time"
)

func TestNewConfig_PingTimeout(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given []Option
		want  time.Duration
	}{
		"default without Timeout": {given: nil, want: defaultPingTimeout},
		"Timeout overrides it":    {given: []Option{Timeout(7 * time.Second)}, want: 7 * time.Second},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := newConfig(tt.given...).pingTimeout

			if got != tt.want {
				t.Fatalf("pingTimeout = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeout_AlsoSetsDialTimeout(t *testing.T) {
	t.Parallel()

	// Given/When
	cfg := newConfig(Timeout(time.Second))

	// Then: Timeout contributes a kgo option (the dial timeout).
	if len(cfg.kgoOpts) != 1 {
		t.Fatalf("kgo options = %d, want 1", len(cfg.kgoOpts))
	}
}
