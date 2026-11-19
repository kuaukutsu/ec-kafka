package config

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	envGroup = "KAFKA_CONSUMER_GROUP"
	envTopic = "KAFKA_CONSUMER_TOPIC"
)

// Not parallel: the test mutates process environment variables.
func TestConfig_ConsumerGroupAndTopic(t *testing.T) {
	type given struct {
		yaml     string
		envGroup string
		envTopic string
	}

	type want struct {
		group string
		topic string
	}

	tests := map[string]struct {
		given given
		want  want
	}{
		"file only": {
			given: given{yaml: "queue:\n  consumer:\n    group: file-group\n    topic: file-topic\n"},
			want:  want{group: "file-group", topic: "file-topic"},
		},
		"env only": {
			given: given{
				yaml:     "queue:\n  seed: localhost:9092\n",
				envGroup: "env-group",
				envTopic: "env-topic",
			},
			want: want{group: "env-group", topic: "env-topic"},
		},
		"env has priority over file": {
			given: given{
				yaml:     "queue:\n  consumer:\n    group: file-group\n    topic: file-topic\n",
				envGroup: "env-group",
				envTopic: "env-topic",
			},
			want: want{group: "env-group", topic: "env-topic"},
		},
		"values are resolved independently": {
			given: given{
				yaml:     "queue:\n  consumer:\n    group: file-group\n    topic: file-topic\n",
				envTopic: "env-topic",
			},
			want: want{group: "file-group", topic: "env-topic"},
		},
		"nothing set": {
			given: given{yaml: "env: local\n"},
			want:  want{},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Given
			setEnv(t, envGroup, tt.given.envGroup)
			setEnv(t, envTopic, tt.given.envTopic)

			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tt.given.yaml), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			// When
			var cfg Config
			if err := cleanenv.ReadConfig(path, &cfg); err != nil {
				t.Fatalf("ReadConfig() error = %v", err)
			}

			// Then
			if cfg.Queue.Consumer.Group != tt.want.group {
				t.Errorf("group = %q, want %q", cfg.Queue.Consumer.Group, tt.want.group)
			}

			if cfg.Queue.Consumer.Topic != tt.want.topic {
				t.Errorf("topic = %q, want %q", cfg.Queue.Consumer.Topic, tt.want.topic)
			}
		})
	}
}

// setEnv sets the variable for the test, or guarantees it is unset for an empty value.
func setEnv(t *testing.T, key, value string) {
	t.Helper()

	t.Setenv(key, value) // registers restoration of the original value

	if value == "" {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

func TestConsumer_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given Consumer
		want  error
	}{
		"valid":         {given: Consumer{Group: "g", Topic: "t"}, want: nil},
		"group missing": {given: Consumer{Topic: "t"}, want: ErrConsumerGroupRequired},
		"topic missing": {given: Consumer{Group: "g"}, want: ErrConsumerTopicRequired},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tt.given.Validate()

			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestQueue_Brokers(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given string
		want  []string
	}{
		"single":                  {given: "localhost:19092", want: []string{"localhost:19092"}},
		"comma separated":         {given: "a:1,b:2", want: []string{"a:1", "b:2"}},
		"spaces around addresses": {given: "a:1, b:2 ,  c:3", want: []string{"a:1", "b:2", "c:3"}},
		"empty items are dropped": {given: "a:1,, ,b:2,", want: []string{"a:1", "b:2"}},
		"empty seed":              {given: "", want: nil},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := Queue{Seed: tt.given}.Brokers()

			if !slices.Equal(got, tt.want) {
				t.Fatalf("Brokers() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQueue_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given string
		want  error
	}{
		"valid":           {given: "localhost:19092", want: nil},
		"valid list":      {given: "a:1, b:2", want: nil},
		"empty":           {given: "", want: ErrSeedRequired},
		"only separators": {given: " , ,", want: ErrSeedRequired},
		"only whitespace": {given: "   ", want: ErrSeedRequired},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := Queue{Seed: tt.given}.Validate()

			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestEnv_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		given Env
		want  error
	}{
		"local":      {given: EnvLocal, want: nil},
		"stage":      {given: EnvStage, want: nil},
		"production": {given: EnvProd, want: nil},
		"typo":       {given: "prodution", want: ErrInvalidEnv},
		"empty":      {given: "", want: ErrInvalidEnv},
		"wrong case": {given: "Local", want: ErrInvalidEnv},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := tt.given.Validate()

			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() error = %v, want %v", err, tt.want)
			}
		})
	}
}
