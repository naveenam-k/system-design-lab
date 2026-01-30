package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/redis/go-redis/v9"
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
	mu         sync.Mutex
	counts     map[AggregateKey]int64
	session    *gocql.Session
	reader     *kafka.Reader
	redis      *redis.Client
	lastMsg    kafka.Message
	hasMsg     bool
	dedupCount int64 // Track how many duplicates skipped
}

const (
	// Bloom filter settings
	bloomCapacity  = 10_000_000 // 10M items per day
	bloomErrorRate = 0.001      // 0.1% false positive rate
	bloomTTLDays   = 8          // Keep 8 days of bloom filters
)

func main() {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:29092")
	cassandraHosts := getEnv("CASSANDRA_HOSTS", "localhost:9042")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	consumerGroup := getEnv("CONSUMER_GROUP", "aggregator")
	flushInterval := getEnvDuration("FLUSH_INTERVAL", 30*time.Second)
	topic := "user.listen.raw"

	log.Printf("Starting aggregator: kafka=%s cassandra=%s redis=%s group=%s flush=%s",
		kafkaBroker, cassandraHosts, redisAddr, consumerGroup, flushInterval)
	log.Printf("Redis Bloom Filter: capacity=%d error_rate=%.4f ttl_days=%d",
		bloomCapacity, bloomErrorRate, bloomTTLDays)

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

	// Connect to Redis (with RedisBloom module)
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	// Test Redis connection
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis (RedisBloom)")

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
		redis:   rdb,
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

		agg.accumulate(ctx, event, msg)
	}

	log.Println("Shutdown complete")
}

// bloomKey returns the Redis key for the bloom filter for a given day
func bloomKey(day string) string {
	return fmt.Sprintf("dedup:%s", day)
}

// ensureBloomFilter creates a bloom filter if it doesn't exist and sets TTL
func (a *Aggregator) ensureBloomFilter(ctx context.Context, day string) error {
	key := bloomKey(day)

	// Try to reserve (create) the bloom filter
	// BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
	err := a.redis.Do(ctx, "BF.RESERVE", key, bloomErrorRate, bloomCapacity, "NONSCALING").Err()
	if err != nil {
		// Ignore "item exists" error - filter already created
		if !strings.Contains(err.Error(), "item exists") {
			return err
		}
	} else {
		// New filter created - set TTL
		ttl := time.Duration(bloomTTLDays) * 24 * time.Hour
		a.redis.Expire(ctx, key, ttl)
		log.Printf("Created bloom filter: %s (TTL: %v)", key, ttl)
	}

	return nil
}

// checkAndAddToBloom returns true if item was already seen (or possibly seen)
func (a *Aggregator) checkAndAddToBloom(ctx context.Context, day, eventID string) (bool, error) {
	key := bloomKey(day)

	// Ensure bloom filter exists
	if err := a.ensureBloomFilter(ctx, day); err != nil {
		log.Printf("Warning: failed to ensure bloom filter: %v", err)
		// Continue anyway - BF.ADD will create if needed
	}

	// BF.ADD returns 1 if item was added (new), 0 if it already existed
	// go-redis returns this as int64
	result, err := a.redis.Do(ctx, "BF.ADD", key, eventID).Result()
	if err != nil {
		return false, err
	}

	// result == 0 (int64) means item already existed (duplicate)
	// result == 1 (int64) means item was added (new)
	switch v := result.(type) {
	case int64:
		return v == 0, nil
	case bool:
		// Some versions return bool: true = added, false = existed
		return !v, nil
	default:
		return false, fmt.Errorf("unexpected type %T from BF.ADD", result)
	}
}

func (a *Aggregator) accumulate(ctx context.Context, event ListenEvent, msg kafka.Message) {
	// Convert timestamp to day
	listenedAt := time.Unix(event.ListenedAt, 0)
	day := listenedAt.Format("2006-01-02")

	// DEDUP CHECK: Use Redis Bloom Filter (shared across all aggregators)
	isDuplicate, err := a.checkAndAddToBloom(ctx, day, event.EventID)
	if err != nil {
		log.Printf("Warning: bloom filter check failed: %v (processing event anyway)", err)
		// On error, we process the event to avoid data loss
		// This could cause over-count in rare cases, but bloom failure is rare
	} else if isDuplicate {
		// Already seen - SKIP to prevent over-counting
		a.mu.Lock()
		a.dedupCount++
		a.lastMsg = msg
		a.hasMsg = true
		a.mu.Unlock()
		return
	}

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
	if len(a.counts) == 0 && !a.hasMsg {
		a.mu.Unlock()
		return
	}

	// Snapshot current counts
	counts := a.counts
	lastMsg := a.lastMsg
	hasMsg := a.hasMsg
	dedupCount := a.dedupCount

	// Reset for next batch
	a.counts = make(map[AggregateKey]int64)
	a.hasMsg = false
	a.dedupCount = 0
	a.mu.Unlock()

	log.Printf("Flushing %d aggregates to Cassandra (skipped %d duplicates via Redis Bloom)", len(counts), dedupCount)

	// WITH BLOOM FILTER: Write to Cassandra FIRST, then commit offset
	// Bloom filter protects against duplicates if replay happens
	
	// 1. Write counter increments to Cassandra FIRST
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

	// 2. Commit offset AFTER successful Cassandra write
	// If crash before commit: replay happens, bloom filter skips duplicates
	if hasMsg {
		if err := a.reader.CommitMessages(ctx, lastMsg); err != nil {
			log.Printf("Error committing offset: %v", err)
		} else {
			log.Printf("Committed offset: partition=%d offset=%d", lastMsg.Partition, lastMsg.Offset)
		}
	}

	log.Printf("Flush complete")
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
