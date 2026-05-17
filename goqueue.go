package goqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/Gurveer1510/goqueue/internal/broker"
	"github.com/Gurveer1510/goqueue/internal/client"
	"github.com/Gurveer1510/goqueue/internal/core/task"
	"github.com/Gurveer1510/goqueue/internal/mux"
	"github.com/Gurveer1510/goqueue/internal/server"
	"github.com/redis/go-redis/v9"
)

type Task = task.Task

type Option = client.Option

func MaxRetry(n int) Option {
	return client.MaxRetry(n)
}

func IdempotencyKey(key string) Option {
	return client.IdempotencyKey(key)
}

type Client struct {
	c *client.Client
}

func NewClient(redisAddr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}
	return &Client{c: client.NewClient(broker.NewRedisBroker(rdb))}, nil
}

func (cl *Client) Enqueue(ctx context.Context, taskType string, payload any, opts ...Option) (*Task, error) {
	return cl.c.Enqueue(ctx, taskType, payload, opts...)
}

func (cl *Client) EnqueueIn(ctx context.Context, d time.Duration, taskType string, payload any, opts ...Option) (*Task, error) {
	return cl.c.EnqueueIn(ctx, d, taskType, payload, opts...)
}

func (cl *Client) EnqueueAt(ctx context.Context, at time.Time, taskType string, payload any, opts ...Option) (*Task, error) {
	return cl.c.EnqueueAt(ctx, at, taskType, payload, opts...)
}

type Server struct {
	mux       *mux.ServeMux
	broker    broker.Broker
	processor *server.Processor
	forwarder *server.Forwarder
	recoverer *server.Recoverer
}

type Config struct {
	RedisAddr   string
	Concurrency int

	// ForwarderInterval controls how often scheduled/retry tasks are promoted
	// to the pending queue. Default: 5 seconds
	ForwarderInterval time.Duration

	// RecovererInterval controls how often stale active tasks are checked
	// Default: 1 minute
	RecovererInterval time.Duration

	// RecovererTimeout marks tasks as stale after this duration
	// Default: 5 minutes
	RecovererTimeout time.Duration

	// BatchSize controls how many tasks are processed per forwarder/recoverer cycle
	// Default: 20
	BatchSize int
}

func NewServer(cfg Config) (*Server, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis unreachable: %v", err)
	}
	b := broker.NewRedisBroker(rdb)
	m := mux.New()
	concurrency := cfg.Concurrency
	if concurrency == 0 {
		concurrency = 10
	}

	forwarderInterval := cfg.ForwarderInterval
	if forwarderInterval == 0 {
		forwarderInterval = 5 * time.Second
	}

	recovererInterval := cfg.RecovererInterval
	if recovererInterval == 0 {
		recovererInterval = time.Minute
	}

	recovererTimeout := cfg.RecovererTimeout
	if recovererTimeout == 0 {
		recovererTimeout = 5 * time.Minute
	}

	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = 20
	}

	return &Server{
		mux:       m,
		broker:    b,
		processor: server.NewProcessor(b, m, concurrency),
		forwarder: server.NewForwarder(rdb, forwarderInterval, batchSize),
		recoverer: server.NewRecoverer(rdb, recovererTimeout, recovererInterval, batchSize),
	}, nil
}

func (s *Server) HandleFunc(taskType string, fn func(context.Context, *task.Task) error) {
	s.mux.HandleFunc(taskType, fn)
}

// Run starts the server's background workers and blocks until the context is cancelled.
// When cancelled, it gracefully drains in-flight tasks (up to 30 seconds).
// The returned done channel closes when shutdown is complete.
func (s *Server) Run(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		go s.forwarder.Start(ctx)
		go s.recoverer.Start(ctx)
		s.processor.Start(ctx)
	}()
	return done
}

// Stats returns current queue statistics.
func (s *Server) Stats(ctx context.Context) (broker.Stats, error) {
	return s.broker.Stats(ctx)
}

// Health returns an error if Redis is unreachable.
func (s *Server) Health(ctx context.Context) error {
	stats, err := s.broker.Stats(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	if stats.Pending < 0 || stats.Active < 0 {
		return fmt.Errorf("invalid queue stats")
	}
	return nil
}
