# Phase 5 — Observability & Operational Infrastructure Deep-Dive

## 1. Objective & Problem Statement

High-throughput infrastructure requires total operational visibility. Operating a single-writer ledger without metrics or profiling is dangerous — queue saturation, slow `fsync` latency, or CPU lock bottlenecks can lead to system degradation without warning.

Phase 5 introduces:
1. **Prometheus Metrics Collection** (`internal/observability/metrics.go`).
2. **Structured Logging via `slog`** (`internal/observability/logger.go`).
3. **Diagnostic & Profiling HTTP Server** (`internal/observability/pprof.go`).
4. **Production Observability Stack** (`docker-compose.yml` + `deploy/prometheus.yml`).

---

## 2. Core Go Language Concepts & Engineering Internals

### A. Zero-Allocation Metrics Collection (`prometheus/client_golang`)
We register static Prometheus metrics initialized at package load time using `promauto`:

```go
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
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 16),
		},
	)
)
```

* **Go Concept (Exponential Buckets)**: Latency histograms use base-2 logarithmic buckets (`0.00005s` to `3.2s`) to capture low microsecond batch execution times alongside high-percentile latency spikes.

---

### B. Structured Contextual Logging with `log/slog`
Go 1.21 introduced `log/slog` for structured logging. We initialize a global logger supporting JSON and Text formatting:

```go
func InitLogger(levelStr string, format string) *slog.Logger {
    var level slog.Level
    switch strings.ToLower(levelStr) {
    case "debug": level = slog.LevelDebug
    case "error": level = slog.LevelError
    default:      level = slog.LevelInfo
    }

    opts := &slog.HandlerOptions{Level: level}
    var handler slog.Handler
    if strings.ToLower(format) == "json" {
        handler = slog.NewJSONHandler(os.Stdout, opts)
    } else {
        handler = slog.NewTextHandler(os.Stdout, opts)
    }

    logger := slog.New(handler)
    slog.SetDefault(logger)
    return logger
}
```

---

### C. Diagnostic pprof Profiling Server
Go's built-in `net/http/pprof` package enables live CPU, heap memory, goroutine, and mutex profiling:

```go
func NewServer(addr string) *Server {
    mux := http.NewServeMux()
    mux.Handle("/metrics", promhttp.Handler())
    mux.HandleFunc("/debug/pprof/", pprof.Index)
    mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
    mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
    mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
    mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
    return &Server{httpServer: &http.Server{Addr: addr, Handler: mux}}
}
```

* **Live CPU Analysis**: Running `go tool pprof http://localhost:6060/debug/pprof/profile?seconds=10` generates flamegraphs directly from the live running process with negligible performance impact.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **Logging Library** | Standard `log/slog` | `zap` / `zerolog` | `slog` is built into Go standard library, eliminating third-party dependency vulnerabilities while matching `zap` performance. |
| **Diagnostic Port** | Separate HTTP Port (`:6060`) | Expose on gRPC port | Separating management/metrics from external client gRPC port prevents exposing sensitive profiling endpoints to public traffic. |
