package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
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

func main() {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:29092")
	cassandraHosts := getEnv("CASSANDRA_HOSTS", "localhost:9042")
	consumerGroup := getEnv("CONSUMER_GROUP", "raw-event-processor")
	topic := "user.listen.raw"

	log.Printf("Starting raw-event-processor: kafka=%s cassandra=%s group=%s",
		kafkaBroker, cassandraHosts, consumerGroup)

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
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()
	log.Printf("Listening on topic: %s", topic)

	// Handle shutdown gracefully
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Process messages
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // context cancelled, shutdown
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}

		var event ListenEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshaling event: %v", err)
			// Commit anyway to skip bad message
			reader.CommitMessages(ctx, msg)
			continue
		}

		// Write to Cassandra
		if err := writeEvent(session, event); err != nil {
			log.Printf("Error writing to Cassandra: %v", err)
			// Don't commit — will retry on restart
			continue
		}

		// Commit offset after successful write
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Error committing offset: %v", err)
		}

		log.Printf("Processed: user=%s song=%s listened_at=%d",
			event.UserID, event.SongID, event.ListenedAt)
	}

	log.Println("Shutdown complete")
}

func writeEvent(session *gocql.Session, event ListenEvent) error {
	// Convert unix timestamp to time.Time
	listenedAt := time.Unix(event.ListenedAt, 0)
	day := listenedAt.Format("2006-01-02") // Cassandra DATE format

	query := `
		INSERT INTO user_listen_history 
			(user_id, day, listened_at, event_id, song_id, provider)
		VALUES (?, ?, ?, ?, ?, ?)
	`

	return session.Query(query,
		event.UserID,
		day,
		listenedAt,
		event.EventID,
		event.SongID,
		event.Provider,
	).Exec()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
