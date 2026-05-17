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

    // Worker process
    srv, err := goqueue.NewServer(goqueue.Config{
        RedisAddr:   "localhost:6379",
        Concurrency: 10,
    })
    if err != nil {
        log.Fatalf("server init failed: %v", err)
    }

    srv.HandleFunc("email:deliver", func(ctx context.Context, t *goqueue.Task) error {
        log.Printf("sending email to %s", t.Payload)
        return nil
    })

    // Producer
    c, err := goqueue.NewClient("localhost:6379")
    if err != nil {
        log.Fatalf("client init failed: %v", err)
    }

    t, err := c.EnqueueIn(ctx, 10*time.Second, "email:deliver",
        map[string]any{"to": "user@example.com"},
        goqueue.MaxRetry(3),
    )
    if err != nil {
        log.Fatalf("enqueue failed: %v", err)
    }
    log.Printf("enqueued task id=%s", t.ID)

    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
    go func() {
        <-quit
        cancel()
    }()

    srv.Run(ctx) // blocks until ctx cancelled
}