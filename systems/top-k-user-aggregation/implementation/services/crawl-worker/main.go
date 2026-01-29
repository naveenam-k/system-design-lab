package main

import (
	"log"
	"os"

	"github.com/hibiken/asynq"
	"github.com/system-design-lab/crawl-worker/tasks"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"crawl": 10,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(tasks.TypeCrawlUser, tasks.HandleCrawlUserTask)

	log.Printf("Starting crawl-worker, redis=%s", redisAddr)
	if err := srv.Run(mux); err != nil {
		log.Fatalf("could not start server: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
