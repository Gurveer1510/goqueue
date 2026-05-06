package broker

import (
	"context"
	"time"

	"github.com/Gurveer1510/goqueue/internal/core/task"
)

type Broker interface {
	Enqueue(ctx context.Context, t *task.Task) error
	Schedule(ctx context.Context, t *task.Task, at time.Time) error
	Dequeue(ctx context.Context, timeout time.Duration) (*task.Task, error)
	Ack(ctx context.Context, t *task.Task) error
	Nack(ctx context.Context, t *task.Task) error
	DeadLetter(ctx context.Context, t *task.Task) error
	UpdateHashSet(ctx context.Context, id string, data []byte) error
}
