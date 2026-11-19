package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// config accumulates the settings collected from Option values.
type config struct {
	kgoOpts     []kgo.Opt
	pingTimeout time.Duration
}

// defaultPingTimeout bounds the connectivity check in New unless Timeout is given.
const defaultPingTimeout = time.Second

// Option modifies config. A single Option may append any number of kgo.Opt.
type Option func(*config)

func newConfig(opts ...Option) *config {
	cfg := &config{pingTimeout: defaultPingTimeout}
	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

// WithKgo passes arbitrary franz-go options through as is.
func WithKgo(opts ...kgo.Opt) Option {
	return func(c *config) {
		c.kgoOpts = append(c.kgoOpts, opts...)
	}
}

func SeedBrokers(seeds ...string) Option {
	return WithKgo(kgo.SeedBrokers(seeds...))
}

// Timeout sets both the dial timeout and the timeout of the connectivity
// check performed by New.
func Timeout(timeout time.Duration) Option {
	return func(c *config) {
		c.pingTimeout = timeout
		WithKgo(kgo.DialTimeout(timeout))(c)
	}
}

func RecordDeliveryTimeout(timeout time.Duration) Option {
	return WithKgo(kgo.RecordDeliveryTimeout(timeout))
}

func ProducerDefault(clientID string) Option {
	return WithKgo(
		kgo.ClientID(clientID),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.LeaderAck()),
	)
}

// ConsumerDefault joins the consumer group and subscribes to the given topics.
func ConsumerDefault(clientID, group string, topics ...string) Option {
	return WithKgo(
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
}
