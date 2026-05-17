package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	forwardScript = redis.NewScript(`
		local id = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
		if #id == 0 then
			return 0
		end
		local removed = redis.call('ZREM', KEYS[1], id[1])
		if removed == 0 then
			return 0
		end
		redis.call('LPUSH', KEYS[2], id[1])
		return 1
	`)
)

type Forwarder struct {
	rdb      *redis.Client
	interval time.Duration
	batch    int
}

func NewForwarder(rdb *redis.Client, interval time.Duration, batch int) *Forwarder {
	if batch <= 0 {
		batch = 20
	}
	return &Forwarder{
		rdb:      rdb,
		interval: interval,
		batch:    batch,
	}
}

func (f *Forwarder) Start(ctx context.Context) {
	log.Printf("Forwarder starting, interval=%s batch=%d", f.interval, f.batch)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := f.forward(ctx); err != nil {
				log.Printf("forwarder error: %v", err)
			}
		case <-ctx.Done():
			log.Println("forwarder shutting down")
			return
		}
	}
}

func (f *Forwarder) forward(ctx context.Context) error {
	now := time.Now().Unix()
	nowFloat := fmt.Sprintf("%d", now)

	totalForwarded := 0
	for _, src := range []string{"scheduled", "retry"} {
		for {
			result, err := forwardScript.Run(ctx, f.rdb, []string{src, "pending"}, nowFloat, f.batch).Int()
			if err != nil {
				log.Printf("forward script error for %s: %v", src, err)
				break
			}
			if result == 0 {
				break
			}
			totalForwarded += result
		}
	}

	if totalForwarded > 0 {
		log.Printf("forwarder promoted %d tasks", totalForwarded)
	}
	return nil
}
