# GoQueue — Usage Guide

GoQueue is a Redis-backed background job queue for Go. This guide covers everything you need to integrate it into your own application.

---

## Installation

```bash
go get github.com/Gurveer1510/goqueue
```

**Requirements**

- Go 1.22+
- Redis 7+

---

## Quickstart

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/Gurveer1510/goqueue"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 1. Create a server and register handlers
    srv, err := goqueue.NewServer(goqueue.Config{
        RedisAddr:   "localhost:6379",
        Concurrency: 10,
    })
    if err != nil {
        log.Fatalf("failed to create server: %v", err)
    }

    srv.HandleFunc("email:deliver", func(t *goqueue.Task) error {
        log.Printf("processing task id=%s", t.ID)
        // your logic here
        return nil
    })

    // 2. Create a client and enqueue tasks
    c, err := goqueue.NewClient("localhost:6379")
    if err != nil {
        log.Fatalf("failed to create client: %v", err)
    }

    _, err = c.Enqueue(ctx, "email:deliver", map[string]any{
        "to": "user@example.com",
    })
    if err != nil {
        log.Fatalf("enqueue failed: %v", err)
    }

    // 3. Run the server (blocks until ctx is cancelled)
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
    go func() {
        <-quit
        cancel()
    }()

    srv.Run(ctx)
}
```

---

## Core Concepts

### Task

A `Task` is the unit of work. It carries a type, a JSON payload, and retry metadata.

```go
type Task struct {
    ID       string          // unique identifier, auto-assigned
    Type     string          // used to route to the correct handler
    Payload  json.RawMessage // arbitrary JSON data
    Retries  int             // how many times this task has been attempted
    MaxRetry int             // maximum attempts before moving to dead-letter
}
```

You read the payload inside your handler by unmarshalling it:

```go
srv.HandleFunc("email:deliver", func(t *goqueue.Task) error {
    var p struct {
        To string `json:"to"`
    }
    if err := json.Unmarshal(t.Payload, &p); err != nil {
        return err
    }
    log.Printf("sending email to %s", p.To)
    return nil
})
```

### Server

The `Server` runs in your worker process. It manages three background loops:

- **Processor** — dequeues tasks from `pending` and dispatches them to handlers concurrently.
- **Forwarder** — promotes scheduled and due-for-retry tasks into the `pending` queue on a 5-second tick.
- **Recoverer** — detects tasks stuck in `active` (e.g. worker crashed) and re-queues them.

### Client

The `Client` runs in your producer process (your web server, CLI, etc). It enqueues tasks into Redis. The client and server can be in the same binary or separate processes — they only share Redis.

---

## Server Configuration

```go
srv, err := goqueue.NewServer(goqueue.Config{
    RedisAddr:   "localhost:6379", // required
    Concurrency: 20,               // defaults to 10 if zero
})
```

| Field | Type | Default | Description |
|---|---|---|---|
| `RedisAddr` | `string` | `""` | Redis address in `host:port` format |
| `Concurrency` | `int` | `10` | Max number of tasks processed simultaneously |

---

## Registering Handlers

Register one handler per task type before calling `Run`. Registering the same type twice will panic.

```go
srv.HandleFunc("email:deliver", func(t *goqueue.Task) error {
    // return nil → task is acknowledged and deleted
    // return err → task is retried with exponential backoff
    return nil
})

srv.HandleFunc("invoice:generate", func(t *goqueue.Task) error {
    return nil
})
```

---

## Enqueueing Tasks

### Immediate

Sends the task to the `pending` queue right away.

```go
t, err := c.Enqueue(ctx, "email:deliver", map[string]any{
    "to": "user@example.com",
})
log.Printf("enqueued: %s", t.ID)
```

### Delayed — run after a duration

```go
t, err := c.EnqueueIn(ctx, 30*time.Minute, "email:deliver", map[string]any{
    "to": "user@example.com",
})
```

### Scheduled — run at a specific time

```go
t, err := c.EnqueueAt(ctx, time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC), "email:deliver", map[string]any{
    "to": "user@example.com",
})
```

---

## Options

Options are passed as the last arguments to any enqueue call.

### `MaxRetry`

Sets the maximum number of processing attempts. Defaults to `3`.

```go
c.Enqueue(ctx, "email:deliver", payload, goqueue.MaxRetry(5))
c.EnqueueIn(ctx, time.Hour, "email:deliver", payload, goqueue.MaxRetry(0)) // no retries
```

---

## Retries & Backoff

When a handler returns an error, the task is automatically retried. Retries use **exponential backoff**:

```
delay = retries² × 10 seconds
```

| Attempt | Delay before next retry |
|---|---|
| 1st failure | 10s |
| 2nd failure | 40s |
| 3rd failure | 90s |

Once `Retries >= MaxRetry`, the task is moved to the **dead-letter queue** instead of being retried again.

---

## Dead-Letter Queue

Tasks that exhaust all retries are moved to a Redis sorted set named `dead`, scored by the time they were dead-lettered. The task payload is preserved so you can inspect or replay it.

You can inspect dead tasks directly in Redis:

```bash
# List dead task IDs
redis-cli ZRANGE dead 0 -1 WITHSCORES

# Inspect a specific task payload
redis-cli HGET tasks:<task-id> data
```

---

## Graceful Shutdown

`srv.Run(ctx)` blocks until `ctx` is cancelled, then waits for all in-flight tasks to finish (up to 30 seconds) before returning. Wire it to OS signals:

```go
ctx, cancel := context.WithCancel(context.Background())

quit := make(chan os.Signal, 1)
signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

go func() {
    <-quit
    log.Println("shutting down...")
    cancel()
}()

srv.Run(ctx) // returns after drain completes
log.Println("shutdown complete")
```

---

## Separating Producer and Worker

In production you will typically run the client (producer) in your API server and the server (worker) as a separate process.

**API server process**

```go
c, err := goqueue.NewClient("redis:6379")
// enqueue tasks from HTTP handlers
```

**Worker process**

```go
srv, err := goqueue.NewServer(goqueue.Config{RedisAddr: "redis:6379"})
srv.HandleFunc("email:deliver", handleEmailDeliver)
srv.Run(ctx)
```

Both connect to the same Redis instance. No other coordination is needed.

---

## Redis Key Reference

GoQueue uses the following Redis keys. Useful when debugging directly in `redis-cli`.

| Key | Type | Description |
|---|---|---|
| `pending` | List | Tasks ready to be processed immediately |
| `scheduled` | Sorted Set | Tasks waiting for their scheduled time (score = Unix timestamp) |
| `retry` | Sorted Set | Failed tasks waiting for their backoff delay |
| `active` | Sorted Set | Tasks currently being processed (score = dequeue time) |
| `dead` | Sorted Set | Tasks that exhausted all retries |
| `tasks:<id>` | Hash | Full JSON payload for a task, keyed by ID |

---

## Start Redis locally

```bash
docker run -p 6379:6379 redis:7
```