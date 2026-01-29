package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/redis/go-redis/v9"
)

// TopKResult is a single song in the Top-K response
type TopKResult struct {
	SongID      string `json:"song_id"`
	ListenCount int64  `json:"listen_count"`
	Rank        int    `json:"rank"`
}

// TopKResponse is the API response
type TopKResponse struct {
	UserID  string       `json:"user_id"`
	Days    int          `json:"days"`
	K       int          `json:"k"`
	Results []TopKResult `json:"results"`
	Cached  bool         `json:"cached"`
}

var (
	cassandraSession *gocql.Session
	redisClient      *redis.Client
	cacheTTL         time.Duration
)

func main() {
	cassandraHosts := getEnv("CASSANDRA_HOSTS", "localhost:9042")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	port := getEnv("PORT", "8080")
	cacheTTL = getEnvDuration("CACHE_TTL", 1*time.Hour)

	log.Printf("Starting api-server: cassandra=%s redis=%s port=%s cacheTTL=%s",
		cassandraHosts, redisAddr, port, cacheTTL)

	// Connect to Cassandra
	cluster := gocql.NewCluster(strings.Split(cassandraHosts, ",")...)
	cluster.Keyspace = "topk"
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 10 * time.Second

	var err error
	cassandraSession, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer cassandraSession.Close()
	log.Println("Connected to Cassandra")

	// Connect to Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis")

	// Routes
	http.HandleFunc("/healthz", healthzHandler)
	http.HandleFunc("/users/", topKHandler)

	log.Printf("Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// topKHandler handles GET /users/{user_id}/topk?days=7&k=10
func topKHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse path: /users/{user_id}/topk
	path := strings.TrimPrefix(r.URL.Path, "/users/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[1] != "topk" {
		http.Error(w, "invalid path, expected /users/{user_id}/topk", http.StatusBadRequest)
		return
	}
	userID := parts[0]

	// Parse query params
	days := getQueryInt(r, "days", 7)
	k := getQueryInt(r, "k", 10)

	if days < 1 || days > 30 {
		http.Error(w, "days must be 1-30", http.StatusBadRequest)
		return
	}
	if k < 1 || k > 100 {
		http.Error(w, "k must be 1-100", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Check cache
	cacheKey := fmt.Sprintf("topk:%s:%d:%d", userID, days, k)
	cached, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write([]byte(cached))
		return
	}

	// Compute Top-K from Cassandra
	results, err := computeTopK(ctx, userID, days, k)
	if err != nil {
		log.Printf("Error computing topk: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	response := TopKResponse{
		UserID:  userID,
		Days:    days,
		K:       k,
		Results: results,
		Cached:  false,
	}

	// Serialize response
	jsonData, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Cache the result
	redisClient.Set(ctx, cacheKey, jsonData, cacheTTL)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Write(jsonData)
}

func computeTopK(ctx context.Context, userID string, days, k int) ([]TopKResult, error) {
	// Generate list of days to query
	today := time.Now().UTC().Truncate(24 * time.Hour)
	dayList := make([]string, days)
	for i := 0; i < days; i++ {
		day := today.AddDate(0, 0, -i)
		dayList[i] = day.Format("2006-01-02")
	}

	// Aggregate counts across days
	songCounts := make(map[string]int64)

	for _, day := range dayList {
		query := `
			SELECT song_id, listen_count 
			FROM user_daily_topk 
			WHERE user_id = ? AND day = ?
		`
		iter := cassandraSession.Query(query, userID, day).Iter()

		var songID string
		var count int64
		for iter.Scan(&songID, &count) {
			songCounts[songID] += count
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("query error for day %s: %w", day, err)
		}
	}

	// Convert to slice and sort
	type songCount struct {
		songID string
		count  int64
	}
	var sorted []songCount
	for songID, count := range songCounts {
		sorted = append(sorted, songCount{songID, count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	// Take top K
	if len(sorted) > k {
		sorted = sorted[:k]
	}

	// Build response
	results := make([]TopKResult, len(sorted))
	for i, sc := range sorted {
		results[i] = TopKResult{
			SongID:      sc.songID,
			ListenCount: sc.count,
			Rank:        i + 1,
		}
	}

	return results, nil
}

func getQueryInt(r *http.Request, key string, defaultVal int) int {
	val := r.URL.Query().Get(key)
	if val == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return i
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
