package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Gurveer1510/goqueue/internal/broker"
	"github.com/Gurveer1510/goqueue/internal/core/task"
	"github.com/google/uuid"
)

type Client struct {
	broker broker.Broker
}

func NewClient(b broker.Broker) *Client {
	return &Client{broker: b}
}

func (c *Client) Enqueue(ctx context.Context, taskType string, payload any, opts ...Option) (*task.Task, error) {
	if taskType == "" {
		return nil, fmt.Errorf("task type cannot be empty")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	
	t := &task.Task{
		ID:       uuid.NewString(),
		Type:     taskType,
		Payload:  json.RawMessage(data),
		MaxRetry: 3,
	}
	
	for _, opt := range opts {
		opt(t)
	}
	
	if err := c.broker.Enqueue(ctx, t); err != nil {
		return nil, fmt.Errorf("enqueue task %s : %w", taskType, err)
	}
	
	return t, nil
}

// EneueueIn schedules a task to run after a delay
func (c *Client) EnqueueIn(ctx context.Context, d time.Duration, taskType string, payload any, opts ...Option) (*task.Task, error) {
	return c.EnqueueAt(ctx, time.Now().Add(d), taskType, payload, opts...)
}

// EneueueAt schedules a task to run at a specific time
func (c *Client) EnqueueAt(ctx context.Context, at time.Time, taskType string, payload any, opts ...Option) (*task.Task, error) {
	if taskType == "" {
		return nil, fmt.Errorf("task type cannot be empty")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	
	t := &task.Task{
		ID:       uuid.NewString(),
		Type:     taskType,
		Payload:  json.RawMessage(data),
		MaxRetry: 3,
	}

	for _, opt := range opts {
		opt(t)
	}

	if err := c.broker.Schedule(ctx, t, at); err != nil {
		return nil, fmt.Errorf("schedule task %s : %w",taskType, err)
	}

	return t, nil
}
