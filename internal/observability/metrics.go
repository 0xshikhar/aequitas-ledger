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
)
