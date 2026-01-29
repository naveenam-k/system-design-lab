package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
)

// ListenEvent matches the event published by crawl-worker
type ListenEvent struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	SongID     string `json:"song_id"`
	Provider   string `json:"provider"`
	ListenedAt int64  `json:"listened_at"`
}

// AggregateKey is the key for in-memory counts
type AggregateKey struct {
	UserID string
	Day    string
	SongID string
}

// Aggregator holds the in-memory state
type Aggregator struct {
	mu       sync.Mutex
	counts   map[AggregateKey]int64
	session  *gocql.Session
	reader   *kafka.Reader
	lastMsg  kafka.Message
	hasMsg   bool
}

func main() {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:29092")
	cassandraHosts := getEnv("CASSANDRA_HOSTS", "localhost:9042")
	consumerGroup := getEnv("CONSUMER_GROUP", "aggregator")
	flushInterval := getEnvDuration("FLUSH_INTERVAL", 30*time.Second)
	topic := "user.listen.raw"

	log.Printf("Starting aggregator: kafka=%s cassandra=%s group=%s flush=%s",
		kafkaBroker, cassandraHosts, consumerGroup, flushInterval)

	// Connect to Cassandra
	cluster := gocql.NewCluster(strings.Split(cassandraHosts, ",")...)
	cluster.Keyspace = "topk"
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer session.Close()
	log.Println("Connected to Cassandra")

	// Create Kafka reader (consumer group)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		GroupID:  consumerGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()
	log.Printf("Listening on topic: %s", topic)

	agg := &Aggregator{
		counts:  make(map[AggregateKey]int64),
		session: session,
		reader:  reader,
	}

	// Handle shutdown gracefully
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Periodic flush goroutine
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				agg.flush(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Shutdown handler
	go func() {
		<-sigChan
		log.Println("Shutting down... flushing remaining counts")
		agg.flush(ctx)
		cancel()
	}()

	// Process messages
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}

		var event ListenEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshaling event: %v", err)
			reader.CommitMessages(ctx, msg)
			continue
		}

		agg.accumulate(event, msg)
	}

	log.Println("Shutdown complete")
}

func (a *Aggregator) accumulate(event ListenEvent, msg kafka.Message) {
	// Convert timestamp to day
	listenedAt := time.Unix(event.ListenedAt, 0)
	day := listenedAt.Format("2006-01-02")

	key := AggregateKey{
		UserID: event.UserID,
		Day:    day,
		SongID: event.SongID,
	}

	a.mu.Lock()
	a.counts[key]++
	a.lastMsg = msg
	a.hasMsg = true
	a.mu.Unlock()
}

func (a *Aggregator) flush(ctx context.Context) {
	a.mu.Lock()
	if len(a.counts) == 0 {
		a.mu.Unlock()
		return
	}

	// Snapshot current counts
	counts := a.counts
	lastMsg := a.lastMsg
	hasMsg := a.hasMsg

	// Reset for next batch
	a.counts = make(map[AggregateKey]int64)
	a.hasMsg = false
	a.mu.Unlock()

	log.Printf("Flushing %d aggregates to Cassandra", len(counts))

	// Write counter increments to Cassandra
	for key, delta := range counts {
		query := `
			UPDATE user_daily_topk
			SET listen_count = listen_count + ?
			WHERE user_id = ? AND day = ? AND song_id = ?
		`
		if err := a.session.Query(query, delta, key.UserID, key.Day, key.SongID).Exec(); err != nil {
			log.Printf("Error updating counter: %v", err)
			// Continue with other updates
		}
	}

	// Commit Kafka offset after successful flush
	if hasMsg {
		if err := a.reader.CommitMessages(ctx, lastMsg); err != nil {
			log.Printf("Error committing offset: %v", err)
		} else {
			log.Printf("Committed offset: partition=%d offset=%d", lastMsg.Partition, lastMsg.Offset)
		}
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return fallback
}
