package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"aequitas-ledger/internal/engine"
)

type Config struct {
	GRPCPort         string
	RESTPort         string
	MetricsPort      string
	ReplicationPort  string
	LedgerRole       string
	PrimaryAddr      string
	LogLevel         string
	LogFormat        string
	WALDir           string
	WALSegmentSize   int64
	SnapshotDir      string
	SnapshotInterval time.Duration
	MaxSnapshotsKept int
	Engine           engine.Config
}

func Load() Config {
	cfg := Config{
		GRPCPort:        getEnv("PORT", "50051"),
		RESTPort:        getEnv("REST_PORT", "8080"),
		MetricsPort:     getEnv("METRICS_PORT", "6060"),
		ReplicationPort: getEnv("REPLICATION_PORT", "17001"),
		// Role comparisons in cmd/server are lowercase; normalize here so
		// LEDGER_ROLE=FOLLOWER in deployment manifests (docker-compose, k8s)
		// means what it says instead of silently booting a second primary.
		LedgerRole:       strings.ToLower(strings.TrimSpace(getEnv("LEDGER_ROLE", "primary"))),
		PrimaryAddr:      getEnv("PRIMARY_ADDR", "localhost:17001"),
		LogLevel:         getEnv("LOG_LEVEL", "info"),
		LogFormat:        getEnv("LOG_FORMAT", "text"),
		WALDir:           getEnv("WAL_DIR", filepath.Join(".", "data", "wal")),
		WALSegmentSize:   getEnvInt64("WAL_SEGMENT_SIZE", 64<<20),
		SnapshotDir:      getEnv("SNAPSHOT_DIR", filepath.Join(".", "data", "snapshots")),
		SnapshotInterval: getEnvDuration("SNAPSHOT_INTERVAL", 60*time.Second),
		MaxSnapshotsKept: getEnvInt("SNAPSHOT_MAX_KEPT", 3),
		Engine:           engine.DefaultConfig(),
	}

	cfg.Engine.RingBufferSize = getEnvInt("RING_BUFFER_SIZE", cfg.Engine.RingBufferSize)
	cfg.Engine.MaxBatchSize = getEnvInt("MAX_BATCH_SIZE", cfg.Engine.MaxBatchSize)
	cfg.Engine.BatchTimeout = getEnvDuration("BATCH_TIMEOUT", cfg.Engine.BatchTimeout)
	cfg.Engine.IdempotencyMaxSize = getEnvInt("IDEMPOTENCY_MAX_SIZE", cfg.Engine.IdempotencyMaxSize)

	return cfg
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if valStr := os.Getenv(key); valStr != "" {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if valStr := os.Getenv(key); valStr != "" {
		if val, err := strconv.ParseInt(valStr, 10, 64); err == nil {
			return val
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if valStr := os.Getenv(key); valStr != "" {
		if d, err := time.ParseDuration(valStr); err == nil {
			return d
		}
	}
	return defaultVal
}
