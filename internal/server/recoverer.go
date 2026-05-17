package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Gurveer1510/goqueue/internal/core/task"
	"github.com/redis/go-redis/v9"
)

type Recoverer struct {
	rdb      *redis.Client
	timeout  time.Duration
	interval time.Duration
	batch    int
}

func NewRecoverer(rdb *redis.Client, timeout, interval time.Duration, batch int) *Recoverer {
	if batch <= 0 {
		batch = 20
	}
	return &Recoverer{
		rdb:      rdb,
		timeout:  timeout,
		interval: interval,
		batch:    batch,
	}
}

func (r *Recoverer) Start(ctx context.Context) {
	log.Printf("recoverer starting, timeout=%s interval=%s", r.timeout, r.interval)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.recover(ctx); err != nil {
				log.Printf("recoverer error : %v", err)
			}
		case <-ctx.Done():
			log.Println("recoverer shutting down")
			return
		}
	}
}

func (r *Recoverer) recover(ctx context.Context) error {
	cutoff := float64(time.Now().Add(-r.timeout).Unix())

	ids, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		ByScore: true,
		Start:   "-inf",
		Stop:    fmt.Sprintf("%f", cutoff),
		Key:     "active",
		Count:   int64(r.batch),
	}).Result()
	if err != nil {
		return fmt.Errorf("zrangeargs active : %v", err)
	}
	if len(ids) == 0 {
		return nil
	}

	log.Printf("recoverer found %d stale tasks", len(ids))

	for _, id := range ids {
		if err := r.recoverOne(ctx, id); err != nil {
			log.Printf("failed to recover task %s: %v", id, err)
		}
	}
	return nil
}

func (r *Recoverer) recoverOne(ctx context.Context, id string) error {
	removed, err := r.rdb.ZRem(ctx, "active", id).Result()
	if err != nil {
		return fmt.Errorf("zrem axtive: %v", err)
	}

	if removed == 0 {
		return nil
	}

	data, err := r.rdb.HGet(ctx, fmt.Sprintf("tasks:%s", id), "data").Bytes()
	if err == redis.Nil {
		log.Printf("recoverer: payload missing for task %s, skipping", id)
		return nil
	}

	if err != nil {
		return fmt.Errorf("hget task payload: %v", err)
	}

	var t task.Task
	if err := json.Unmarshal(data, &t); err != nil {
		// bad payload
		r.rdb.Del(ctx, fmt.Sprintf("tasks:%s", id))
		return fmt.Errorf("unmarshal task %s: %v", id, err)
	}

	t.Retries++

	if t.Retries >= t.MaxRetry {
		log.Printf("recoverer: task %s exhausted retries — discarding", id)
		pipe := r.rdb.Pipeline()
		pipe.HSet(ctx, fmt.Sprintf("tasks:%s", id), "data", data)
		pipe.ZRem(ctx, "active", id)
		pipe.ZAdd(ctx, "dead", redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: t.ID,
		})
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("recoverer: failed to dead-letter task %s: %v", id, err)
		}
		return nil
	}

	updated, err := json.Marshal(&t)
	if err != nil {
		return fmt.Errorf("marshal updated task: %v", err)
	}

	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, fmt.Sprintf("tasks:%s", id), "data", updated)
	pipe.LPush(ctx, "pending", id)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("re-queue task %s: %v", id, err)
	}
	log.Printf("recoverer: re-queued task %s (retries=%d)", id, t.Retries)
	return nil
}
