package config

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Env string

const (
	EnvLocal Env = "local"
	EnvStage Env = "stage"
	EnvProd  Env = "production"
)

type Config struct {
	Env   Env   `yaml:"env" env-default:"production"`
	Queue Queue `yaml:"queue"`
}

type Queue struct {
	Seed            string        `yaml:"seed" env:"KAFKA_SEED"`
	Timeout         time.Duration `yaml:"timeout" env-default:"5s"`
	DeliveryTimeout time.Duration `yaml:"delivery_timeout" env-default:"10s"`
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

	return &cfg
}

func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("env", string(c.Env)),

		slog.Group("queue",
			slog.String("seed", c.Queue.Seed),
			slog.Duration("timeout", c.Queue.Timeout),
			slog.Duration("delivery_timeout", c.Queue.DeliveryTimeout),
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
