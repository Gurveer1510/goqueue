# Changelog

All notable changes to GoQueue are documented in this file.

## [Unreleased]

### Breaking Changes

- **Handler signature change** - Handlers now receive `context.Context` as the first parameter:
  ```go
  // Old
  srv.HandleFunc("email:deliver", func(t *goqueue.Task) error { ... })

  // New
  srv.HandleFunc("email:deliver", func(ctx context.Context, t *goqueue.Task) error { ... })
  ```

### Critical Bug Fixes

1. **Forwarder task loss** (`internal/server/forwarder.go`)
   - Problem: When `ZRem` succeeded but `LPush` failed, tasks were removed from scheduled/retry but never reached pending — effectively lost forever.
   - Fix: Replaced separate ZRem + LPush calls with an atomic Lua script that performs both operations in a single Redis transaction.

2. **Graceful shutdown not enforced** (`internal/server/processor.go`)
   - Problem: The README claimed a 30-second drain timeout, but `drain()` only sent signals to the semaphore without waiting for goroutines to complete.
   - Fix: Added `sync.WaitGroup` to track in-flight tasks and respect a 30-second timeout. The drain now properly waits for all running tasks to finish.

3. **Context ignored by HandlerFunc** (`internal/core/task/task.go`)
   - Problem: The `HandlerFunc` received `ctx` but discarded it, making it impossible to implement timeouts or cancellation in handlers.
   - Fix: Updated `HandlerFunc` signature to accept context and pass it to the underlying function.

4. **Recoverer dead-letter doesn't update task hash** (`internal/server/recoverer.go`)
   - Problem: When moving exhausted tasks to dead-letter, the task hash wasn't updated with the latest task data.
   - Fix: Now uses a pipeline to update the hash, remove from active, and add to dead in one atomic operation.

5. **Client doesn't validate task type** (`internal/client/client.go`)
   - Problem: `Enqueue` and `EnqueueAt` accepted empty task types, creating invalid tasks.
   - Fix: Added validation that returns an error if task type is empty.

### New Features

1. **Configurable intervals and batch sizes** (`goqueue.go`)
   - Added new `Config` fields:
     - `ForwarderInterval` - How often scheduled/retry tasks are promoted (default: 5 seconds)
     - `RecovererInterval` - How often stale active tasks are checked (default: 1 minute)
     - `RecovererTimeout` - Duration after which tasks are considered stale (default: 5 minutes)
     - `BatchSize` - Tasks processed per forwarder/recoverer cycle (default: 20)

2. **Observability** (`goqueue.go`)
   - Added `Stats() (broker.Stats, error)` method to Server for queue statistics:
     ```go
     stats, err := srv.Stats(ctx)
     // stats.Pending, stats.Scheduled, stats.Active, stats.Retry, stats.Dead
     ```
   - Added `Health() error` method for health checks

3. **Idempotency keys** (`internal/client/option.go`, `internal/broker/redis.go`)
   - New `goqueue.IdempotencyKey(key)` option prevents duplicate task creation:
     ```go
     c.Enqueue(ctx, "email:deliver", payload, goqueue.IdempotencyKey("order-123"))
     ```
   - Uses Redis keys `idempotency:<key>` to track existing tasks
   - Returns an error if a task with the same idempotency key already exists

### Internal Improvements

1. **Server struct now uses Broker interface** (`goqueue.go`)
   - Changed `broker` field from `*broker.RedisBroker` to `broker.Broker` interface
   - Allows for custom broker implementations in tests and alternate backends

2. **Broker interface extended** (`internal/broker/broker.go`)
   - Added `Stats(ctx context.Context) (Stats, error)` method to the Broker interface
   - Added `Stats` struct with queue counts

3. **Redis constants centralized** (`internal/broker/redis.go`)
   - Added `keyIdempotency` constant for idempotency key prefix

4. **Recoverer batch size configurable** (`internal/server/recoverer.go`)
   - Now uses configurable batch size instead of hardcoded 20

5. **Forwarder script** (`internal/server/forwarder.go`)
   - Uses named script variable `forwardScript` for clarity
   - Processes tasks in a loop until no more are available

### Documentation

- Added `Run()` method documentation clarifying that it blocks until context cancellation and returns a done channel
- Updated README example to use new handler signature with context