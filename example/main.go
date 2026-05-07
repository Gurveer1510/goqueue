package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Gurveer1510/goqueue/internal/broker"
	"github.com/Gurveer1510/goqueue/internal/client"
	"github.com/Gurveer1510/goqueue/internal/core/task"
	"github.com/Gurveer1510/goqueue/internal/mux"
	"github.com/Gurveer1510/goqueue/internal/server"
	"github.com/redis/go-redis/v9"
)

func main() {

	// loc, err := time.LoadLocation("Asia/Kolkata")
	// if err != nil {
	// 	log.Fatalf("unknown timezone: %v", err)
	// }

	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis unreachable: %v", err)
	}
	log.Println("redis connected")

	b := broker.NewRedisBroker(rdb)
	c := client.NewClient(b)
	m := mux.New()

	m.HandleFunc("email:deliver", func(t *task.Task) error {
		log.Printf("sending email invite....")
		time.Sleep(2 * time.Second)
		return fmt.Errorf("TESTING ERROR")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.NewForwarder(rdb, 5*time.Second).Start(ctx)
	go server.NewRecoverer(rdb, 5*time.Minute, 1*time.Minute).Start(ctx)

	proc := server.NewProcessor(b, m, 10)

	procDone := make(chan struct{})
	go func() {
		proc.Start(ctx)
		close(procDone)
	}()
	// year int, month time.Month, day int, hour int, min int, sec int, nsec int, loc *time.Location
	// t, err := c.EnqueueAt(ctx, time.Date(2026, 5, 6, 15, 11, 0, 0, loc), "email:deliver", map[string]any{"to": "user@abc.xyz"}, client.MaxRetry(3))
	t, err := c.EnqueueIn(ctx, 10*time.Second, "email:deliver", map[string]any{"to": "user@abc.xyz"}, client.MaxRetry(3))
	// t, err := c.Enqueue(ctx, "email:deliver", map[string]any{"to": "user@abc.xyz"}, client.MaxRetry(3))
	if err != nil {
		log.Printf("enqueue failed: %v", err)
	} else {
		// log.Printf("enqueued task id=%s", t.ID)
		log.Printf("enqueued task id=%s", t.ID)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	sig := <-quit
	log.Printf("received signa %s - shutting down", sig)

	cancel()

	select {
	case <-procDone:
		log.Println("processor drained - clean shutdown")
	case <-time.After(30 * time.Second):
		log.Println("drain timeout - forcing shutdown")
	}
}
