package task

import (
	"context"
	"encoding/json"
)

type Task struct {
	ID              string          `json:"id,omitempty"`
	Type            string          `json:"type,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Retries         int             `json:"retries,omitempty"`
	MaxRetry        int             `json:"max_retry,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
}

type Handler interface {
	ProcessTask(ctx context.Context,t *Task) error
}

type HandlerFunc func(ctx context.Context, t *Task) error

func (f HandlerFunc) ProcessTask(ctx context.Context, t *Task) error {
	return f(ctx, t)
}