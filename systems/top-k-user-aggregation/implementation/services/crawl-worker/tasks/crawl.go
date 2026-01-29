package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/segmentio/kafka-go"
)

const TypeCrawlUser = "crawl:user"

// CrawlUserPayload is the job payload
type CrawlUserPayload struct {
	UserID   string `json:"user_id"`
	Provider string `json:"provider"`
	Since    int64  `json:"since"` // unix timestamp
}

// ListenEvent is the normalized event we publish to Kafka
type ListenEvent struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	SongID     string `json:"song_id"`
	Provider   string `json:"provider"`
	ListenedAt int64  `json:"listened_at"`
}

// NewCrawlUserTask creates a new crawl task
func NewCrawlUserTask(userID, provider string, since time.Time) (*asynq.Task, error) {
	payload, err := json.Marshal(CrawlUserPayload{
		UserID:   userID,
		Provider: provider,
		Since:    since.Unix(),
	})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeCrawlUser, payload), nil
}

// HandleCrawlUserTask processes the crawl job
func HandleCrawlUserTask(ctx context.Context, t *asynq.Task) error {
	var p CrawlUserPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	log.Printf("Crawling user=%s provider=%s since=%d", p.UserID, p.Provider, p.Since)

	// 1. Fetch listen history from provider (simulated for now)
	events := fetchListenHistory(p.UserID, p.Provider, p.Since)

	// 2. Publish events to Kafka
	if err := publishEvents(ctx, events); err != nil {
		return fmt.Errorf("publish events: %w", err)
	}

	// 3. Reschedule for tomorrow
	if err := reschedule(p.UserID, p.Provider); err != nil {
		log.Printf("Warning: failed to reschedule: %v", err)
	}

	log.Printf("Crawl complete: user=%s events=%d", p.UserID, len(events))
	return nil
}

// fetchListenHistory simulates fetching from a provider API
// TODO: replace with real provider API calls
func fetchListenHistory(userID, provider string, since int64) []ListenEvent {
	// Simulated: generate some fake events
	var events []ListenEvent
	now := time.Now().Unix()
	for i := 0; i < 10; i++ {
		events = append(events, ListenEvent{
			EventID:    fmt.Sprintf("%s-%s-%d-%d", userID, provider, now, i),
			UserID:     userID,
			SongID:     fmt.Sprintf("song-%d", i%100),
			Provider:   provider,
			ListenedAt: since + int64(i*3600), // 1 hour apart
		})
	}
	return events
}

// publishEvents sends events to Kafka topic user.listen.raw
func publishEvents(ctx context.Context, events []ListenEvent) error {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:29092")
	topic := "user.listen.raw"

	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.Hash{}, // partition by key (user_id)
	}
	defer w.Close()

	var msgs []kafka.Message
	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(e.UserID),
			Value: data,
		})
	}

	return w.WriteMessages(ctx, msgs...)
}

// reschedule enqueues the next crawl for tomorrow
func reschedule(userID, provider string) error {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()

	task, err := NewCrawlUserTask(userID, provider, time.Now())
	if err != nil {
		return err
	}

	tomorrow := time.Now().Add(24 * time.Hour)
	_, err = client.Enqueue(task,
		asynq.ProcessAt(tomorrow),
		asynq.Queue("crawl"),
	)
	return err
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
