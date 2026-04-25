package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TransfersTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "aequitas",
			Subsystem: "engine",
			Name:      "transfers_total",
			Help:      "Total number of transfers processed by state status.",
		},
		[]string{"status"},
	)

	TransferDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "aequitas",
			Subsystem: "engine",
			Name:      "transfer_duration_seconds",
			Help:      "Duration of transfer processing in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 16), // 50us to ~3.2s
		},
	)

	BatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "aequitas",
			Subsystem: "engine",
			Name:      "batch_size",
			Help:      "Histogram of engine batch sizes.",
			Buckets:   prometheus.LinearBuckets(1, 500, 20),
		},
	)

	WALSyncDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "aequitas",
			Subsystem: "wal",
			Name:      "sync_duration_seconds",
			Help:      "Duration of WAL sync/fsync operations in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 16), // 100us to ~6.5s
		},
	)

	RingBufferDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "aequitas",
			Subsystem: "engine",
			Name:      "ringbuffer_depth",
			Help:      "Current number of events queued in the engine ring buffer.",
		},
	)

	// InvariantViolations must stay at zero forever; any increment means
	// stored account state contradicts the ledger's core invariant and is
	// the single most important alert in the system.
	InvariantViolations = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "aequitas",
			Subsystem: "engine",
			Name:      "invariant_violations_total",
			Help:      "Accounts observed whose posted debits exceed posted credits. Must always be zero.",
		},
	)

	// LastSnapshotLSN reports the snapshot watermark of the background
	// checkpointer; a value stuck at 0 while transfers flow means WAL
	// retention is not running.
	LastSnapshotLSN = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "aequitas",
			Subsystem: "engine",
			Name:      "last_snapshot_lsn",
			Help:      "LSN of the most recent successful snapshot.",
		},
	)

	// Replication metrics (C0.8.5): lag/health on the follower, connection
	// count on the primary.
	ReplicationFollowerLastLSN = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "aequitas",
			Subsystem: "replication",
			Name:      "follower_last_lsn",
			Help:      "Highest primary LSN the follower has durably applied.",
		},
	)
	ReplicationFollowerLag = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "aequitas",
			Subsystem: "replication",
			Name:      "follower_lag",
			Help:      "Primary head LSN minus follower applied LSN (from HEAD announcements).",
		},
	)
	ReplicationFollowerConnected = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "aequitas",
			Subsystem: "replication",
			Name:      "follower_connected",
			Help:      "1 while the follower holds a validated replication stream, else 0.",
		},
	)
	ReplicationFollowerReconnects = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "aequitas",
			Subsystem: "replication",
			Name:      "follower_reconnects_total",
			Help:      "Replication connection attempts after a lost or failed stream.",
		},
	)
	ReplicationPrimaryFollowers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "aequitas",
			Subsystem: "replication",
			Name:      "primary_active_followers",
			Help:      "Number of follower connections currently streaming from this primary.",
		},
	)
)
