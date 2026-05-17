package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Gurveer1510/goqueue/internal/core/task"
	"github.com/redis/go-redis/v9"
)

const (
	keyPending      = "pending"
	keyScheduled    = "scheduled"
	keyRetry        = "retry"
	keyActive       = "active"
	keyTaskFmt      = "tasks:%s"
	keyDead         = "dead"
	keyIdempotency  = "idempotency:%s"
)

type RedisBroker struct {
	rdb *redis.Client
}

func NewRedisBroker(rdb *redis.Client) *RedisBroker {
	return &RedisBroker{
		rdb: rdb,
	}
}

func (b *RedisBroker) Enqueue(ctx context.Context, t *task.Task) error {
	if t.IdempotencyKey != "" {
		exists, err := b.rdb.Exists(ctx, fmt.Sprintf(keyIdempotency, t.IdempotencyKey)).Result()
		if err != nil {
			return fmt.Errorf("check idempotency: %w", err)
		}
		if exists == 1 {
			return fmt.Errorf("task with idempotency key %q already exists", t.IdempotencyKey)
		}
	}

	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	pipe := b.rdb.Pipeline()
	pipe.HSet(ctx, fmt.Sprintf(keyTaskFmt, t.ID), "data", data)
	pipe.LPush(ctx, keyPending, t.ID)
	if t.IdempotencyKey != "" {
		pipe.Set(ctx, fmt.Sprintf(keyIdempotency, t.IdempotencyKey), t.ID, 0)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) Schedule(ctx context.Context, t *task.Task, at time.Time) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	pipe := b.rdb.Pipeline()
	pipe.HSet(ctx, fmt.Sprintf(keyTaskFmt, t.ID), "data", data)
	pipe.ZAdd(ctx, keyScheduled, redis.Z{
		Score:  float64(at.Unix()),
		Member: t.ID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) Dequeue(ctx context.Context, timeout time.Duration) (*task.Task, error) {
	result, err := b.rdb.BRPop(ctx, timeout, keyPending).Result()
	if err != nil {
		return nil, err
	}

	id := result[1]

	err = b.rdb.ZAdd(ctx, keyActive, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: id,
	}).Err()
	if err != nil {
		b.rdb.LPush(ctx, keyPending, id)
		return nil, fmt.Errorf("mark active: %w", err)
	}

	data, err := b.rdb.HGet(ctx, fmt.Sprintf(keyTaskFmt, id), "data").Bytes()
	if err != nil {
		return nil, fmt.Errorf("fetch task payload %s : %w", id, err)
	}

	var t task.Task

	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &t, nil
}

func (b *RedisBroker) Ack(ctx context.Context, t *task.Task) error {
	pipe := b.rdb.Pipeline()
	pipe.ZRem(ctx, keyActive, t.ID)
	pipe.Del(ctx, fmt.Sprintf(keyTaskFmt, t.ID))
	_, err := pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) Nack(ctx context.Context, t *task.Task) error {
	retryAt := time.Now().Add(backoff(t.Retries))
	updated, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	pipe := b.rdb.Pipeline()
	pipe.HSet(ctx, fmt.Sprintf(keyTaskFmt, t.ID), "data", updated)
	pipe.ZRem(ctx, keyActive, t.ID)
	pipe.ZAdd(ctx, keyRetry, redis.Z{Score: float64(retryAt.Unix()), Member: t.ID})
	_, err = pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) DeadLetter(ctx context.Context, t *task.Task) error {
	updated, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	pipe := b.rdb.Pipeline()

	pipe.HSet(ctx, fmt.Sprintf(keyTaskFmt, t.ID), "data", updated)
	pipe.ZRem(ctx, keyActive, t.ID)
	pipe.ZAdd(ctx, keyDead, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: t.ID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

func backoff(retries int) time.Duration {
	return time.Duration(retries*retries) * 10 * time.Second
}

func (b *RedisBroker) Stats(ctx context.Context) (Stats, error) {
	pipe := b.rdb.Pipeline()
	llenPending := pipe.LLen(ctx, keyPending)
	zcardScheduled := pipe.ZCard(ctx, keyScheduled)
	zcardActive := pipe.ZCard(ctx, keyActive)
	zcardRetry := pipe.ZCard(ctx, keyRetry)
	zcardDead := pipe.ZCard(ctx, keyDead)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return Stats{}, fmt.Errorf("stats pipeline: %w", err)
	}

	return Stats{
		Pending:   llenPending.Val(),
		Scheduled: zcardScheduled.Val(),
		Active:    zcardActive.Val(),
		Retry:     zcardRetry.Val(),
		Dead:      zcardDead.Val(),
	}, nil
}
