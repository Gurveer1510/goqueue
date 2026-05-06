package client

import "github.com/Gurveer1510/goqueue/internal/core/task"

type Option func(*task.Task)

func MaxRetry(n int) Option {
	return func(t *task.Task) {
		t.MaxRetry = n
	}
}