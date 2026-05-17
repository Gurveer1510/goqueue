package server

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Gurveer1510/goqueue/internal/broker"
	"github.com/Gurveer1510/goqueue/internal/core/task"
	"github.com/Gurveer1510/goqueue/internal/mux"
	"github.com/redis/go-redis/v9"
)

const drainTimeout = 30 * time.Second

type Processor struct {
	broker      broker.Broker
	mux         *mux.ServeMux
	semaphore   chan struct{}
	concurrency int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewProcessor(b broker.Broker, m *mux.ServeMux, concurrency int) *Processor {
	if concurrency <= 0 {
		concurrency = 10 // check
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Processor{
		broker:      b,
		mux:         m,
		semaphore:   make(chan struct{}, concurrency),
		concurrency: concurrency,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (p *Processor) Start(ctx context.Context) {
	log.Printf("Processor starting with concurrency = %d", p.concurrency)
	p.ctx = ctx

	for {
		select {
		case <-p.ctx.Done():
			log.Println("processor shutting down")
			p.drain()
			return
		default:
		}

		select {
		case p.semaphore <- struct{}{}:
			// got a slot, so proceed
		case <-p.ctx.Done():
			p.drain()
			return
		}

		t, err := p.broker.Dequeue(p.ctx, 5*time.Second)
		if err != nil {
			<-p.semaphore // release the lock
			if errors.Is(err, redis.Nil) {
				continue
			}
			if p.ctx.Err() != nil {
				p.drain()
				return
			}
			log.Printf("dequeue err: %v", err)
			time.Sleep(time.Second) // backoff
			continue
		}

		p.wg.Add(1)
		go p.process(p.ctx, t)
	}
}

func (p *Processor) process(ctx context.Context, t *task.Task) {
	defer func() {
		<-p.semaphore
		p.wg.Done()
	}()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC processing task id=%s type=%s: %v", t.ID, t.Type, r)
			if err := p.broker.Nack(ctx, t); err != nil {
				log.Printf("nack after panic failed: %v", err)
			}
		}
	}()

	h, ok := p.mux.Handler(t.Type)
	if !ok {
		log.Printf("ERROR: no handler registered for type=%q task=%s -- discarding", t.Type, t.ID)
		if err := p.broker.Ack(ctx, t); err != nil {
			log.Printf("acl (no handler) failed: %v", err)
		}
		return
	}

	if err := h.ProcessTask(ctx, t); err != nil {
		log.Printf("task failed id=%s type=%s retries=%d err=%v", t.ID, t.Type, t.Retries, err)

		t.Retries++

		if t.Retries >= t.MaxRetry {
			log.Printf("task exhasuted retries id=%s type=%s -- discarding", t.ID, t.Type)
			if err := p.broker.DeadLetter(ctx, t); err != nil {
				log.Printf("ack (exhausted) failed: %v", err)
			}
			return
		}

		if err := p.broker.Nack(ctx, t); err != nil {
			log.Printf("nack failed id=%s: %v", t.ID, err)
		}
		return
	}

	log.Printf("task completed id=%s type=%s", t.ID, t.Type)
	if err := p.broker.Ack(ctx, t); err != nil {
		log.Printf("ack failed id=%s: %v", t.ID, err)
	}
}

func (p *Processor) drain() {
	log.Println("draining in-flight tasks...")

	// Release all semaphore slots to unblock goroutines
	for i := 0; i < p.concurrency; i++ {
		p.semaphore <- struct{}{}
	}

	// Wait for in-flight tasks with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("drain complete - all tasks finished")
	case <-time.After(drainTimeout):
		log.Printf("drain timeout after %v - %d tasks may still be running", drainTimeout, p.concurrency)
	}
}
