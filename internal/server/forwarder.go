package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type Forwarder struct {
	rdb      *redis.Client
	interval time.Duration
}

func NewForwarder(rdb *redis.Client, interval time.Duration) *Forwarder {
	return &Forwarder{
		rdb:      rdb,
		interval: interval,
	}
}

func (f *Forwarder) Start(ctx context.Context) {
	log.Printf("Forwarder starting, interval=%s", f.interval)
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
	now := float64(time.Now().Unix())

	// ids, err := f.rdb.ZRangeArgs(ctx, ).Result()
	for _, src := range []string{"scheduled", "retry"} {
		ids, err := f.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
			ByScore: true,
			Start:   "-inf",
			Stop:    fmt.Sprintf("%f", now),
			Key:     src,
			Count:   20,
		}).Result()

		if err != nil {
			return fmt.Errorf("zrangebyscore: %w", err)
		}

		if len(ids) == 0 {
			return nil
		}

		log.Printf("forwarder promoting %d due tasks", len(ids))

		for _, id := range ids {
			removed, err := f.rdb.ZRem(ctx, "scheduled", id).Result()
			if err != nil {
				log.Printf("zrem failed for %s: %v", id, err)
				continue
			}
			if removed == 0 {
				continue
			}

			if err := f.rdb.LPush(ctx, "pending", id).Err(); err != nil {
				log.Printf("lpush failed for %s: %v", id, err)
				// if this push fails then the task is lost forever, need to fix this. right now don't know how to do it. too lazy to search
				continue
			}
		}
	}

	return nil
}
