package client

import "github.com/Gurveer1510/goqueue/internal/core/task"

type Option func(*task.Task)

func MaxRetry(n int) Option {
	return func(t *task.Task) {
		t.MaxRetry = n
	}
}

// IdempotencyKey sets a unique key to prevent duplicate task creation.
// If a task with the same key was already enqueued, it returns the existing task.
func IdempotencyKey(key string) Option {
	return func(t *task.Task) {
		t.IdempotencyKey = key
	}
}