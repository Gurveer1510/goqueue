# GoQueue

GoQueue is a Redis-backed background job queue written in Go.

It is built to show how a real async task system works under the hood. The project follows the same execution model used by systems like Asynq: a producer creates tasks, Redis stores and routes them, workers process them, and recovery loops keep the system reliable when things fail.



## Running locally

### Requirements

* Go 1.22+
* Redis 7+

### Start Redis

```bash
docker run -p 6379:6379 redis:7
```

### Run

```bash
go run main.go
```

---

## Future improvements

* Lua for atomic dequeue transitions
* Dead-letter inspection UI
* metrics and tracing
* priority queues
* cron scheduling
* multi-queue routing
* distributed coordination
* task deduplication
* observability dashboards

---

