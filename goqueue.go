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
	return &Server{
		mux:       m,
		broker:    b,
		processor: server.NewProcessor(b, m, concurrency),
		forwarder: server.NewForwarder(rdb, 5*time.Second),
		recoverer: server.NewRecoverer(rdb, 5*time.Minute, time.Minute),
	}, nil
}

func (s *Server) HandleFunc(taskType string, fn func(*task.Task) error) {
	s.mux.HandleFunc(taskType, fn)
}

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
