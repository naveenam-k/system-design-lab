package main

import (
	"log"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/system-design-lab/crawl-worker/tasks"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()

	// Enqueue a test crawl job for "user-123" on "spotify"
	task, err := tasks.NewCrawlUserTask("user-123", "spotify", time.Now().Add(-24*time.Hour))
	if err != nil {
		log.Fatalf("Failed to create task: %v", err)
	}

	info, err := client.Enqueue(task, asynq.Queue("crawl"))
	if err != nil {
		log.Fatalf("Failed to enqueue task: %v", err)
	}

	log.Printf("Enqueued task: id=%s queue=%s", info.ID, info.Queue)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
