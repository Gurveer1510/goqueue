package mux

import (
	"fmt"
	"sync"

	"github.com/Gurveer1510/goqueue/internal/core/task"
)

type ServeMux struct {
	mu       sync.RWMutex
	handlers map[string]task.Handler
}

func New() *ServeMux {
	return &ServeMux{
		handlers: make(map[string]task.Handler),
	}
}

func (m *ServeMux) Handle(taskType string, h task.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if taskType == "" {
		panic("taskqueue: task type cannot be empty")
	}

	if h == nil {
		panic("taskqueue: handler cannot be nil")
	}

	if _, exists := m.handlers[taskType]; exists {
		panic(fmt.Sprintf("taskqueue: hander already registered for type %q", taskType))
	}

	m.handlers[taskType] = h
}

func (m *ServeMux) HandleFunc(taskType string, fn func(*task.Task) error) {
	m.Handle(taskType, task.HandlerFunc(fn))
}

func (m *ServeMux) Handler(taskType string) (task.Handler, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.handlers[taskType]
	return h, ok
}
