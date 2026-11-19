package config

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Env string

const (
	EnvLocal Env = "local"
	EnvStage Env = "stage"
	EnvProd  Env = "production"
)

// ErrInvalidEnv is returned when Config.Env is not one of the known environments.
var ErrInvalidEnv = errors.New("env must be one of: local, stage, production")

// Validate checks that the environment is one of the known values, so a typo
// does not silently select the default logging behavior.
func (e Env) Validate() error {
	switch e {
	case EnvLocal, EnvStage, EnvProd:
		return nil
	default:
		return fmt.Errorf("%w (got %q)", ErrInvalidEnv, string(e))
	}
}

type Config struct {
	Env   Env   `yaml:"env" env-default:"production"`
	Queue Queue `yaml:"queue"`
}

type Queue struct {
	Seed            string        `yaml:"seed" env:"KAFKA_SEED"`
	Timeout         time.Duration `yaml:"timeout" env-default:"5s"`
	DeliveryTimeout time.Duration `yaml:"delivery_timeout" env-default:"10s"`
	Consumer        Consumer      `yaml:"consumer"`
}

// Brokers returns the seed brokers from the comma-separated Seed value,
// trimming spaces and dropping empty items.
func (q Queue) Brokers() []string {
	var brokers []string

	for _, addr := range strings.Split(q.Seed, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			brokers = append(brokers, addr)
		}
	}

	return brokers
}

// ErrSeedRequired is returned when no seed broker is configured. Without it
// franz-go silently falls back to 127.0.0.1:9092.
var ErrSeedRequired = errors.New("queue.seed is required (config file or KAFKA_SEED)")

// Validate checks the settings every Kafka client (producer or consumer)
// cannot start without.
func (q Queue) Validate() error {
	if len(q.Brokers()) == 0 {
		return ErrSeedRequired
	}

	return nil
}

var (
	ErrConsumerGroupRequired = errors.New("queue.consumer.group is required (config file or KAFKA_CONSUMER_GROUP)")
	ErrConsumerTopicRequired = errors.New("queue.consumer.topic is required (config file or KAFKA_CONSUMER_TOPIC)")
)

// Consumer holds consumer-group settings. A value from the environment
// takes priority over the one from the config file.
type Consumer struct {
	Group string `yaml:"group" env:"KAFKA_CONSUMER_GROUP"`
	Topic string `yaml:"topic" env:"KAFKA_CONSUMER_TOPIC"`
}

// Validate checks the settings a consumer cannot start without.
func (c Consumer) Validate() error {
	if c.Group == "" {
		return ErrConsumerGroupRequired
	}

	if c.Topic == "" {
		return ErrConsumerTopicRequired
	}

	return nil
}

func New() *Config {
	cfgPath := fetchConfigPath()
	if cfgPath == "" {
		panic("CONFIG_PATH is not set")
	}

	var cfg Config
	if err := cleanenv.ReadConfig(cfgPath, &cfg); err != nil {
		panic(err)
	}

	if err := cfg.Env.Validate(); err != nil {
		panic(err)
	}

	return &cfg
}

func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("env", string(c.Env)),

		slog.Group("queue",
			slog.String("seed", c.Queue.Seed),
			slog.Duration("timeout", c.Queue.Timeout),
			slog.Duration("delivery_timeout", c.Queue.DeliveryTimeout),
			slog.Group("consumer",
				slog.String("group", c.Queue.Consumer.Group),
				slog.String("topic", c.Queue.Consumer.Topic),
			),
		),
	)
}

func fetchConfigPath() string {
	var res string

	flag.StringVar(&res, "config", "", "path to config file")
	flag.Parse()

	if res == "" {
		res = os.Getenv("CONFIG_PATH")
	}

	return res
}
