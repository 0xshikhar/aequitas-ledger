# 10 — Observability, Config & Lifecycle

**Files in focus:** `internal/observability/`, `internal/config/config.go`,
`cmd/server/main.go`

Production Go is mostly plumbing: what the process logs, measures, and how
it starts and stops. This document covers the small amount of code that
makes the rest operable — and the failure modes (a gauge nobody sets, a
config nobody validates) that turn plumbing into incidents.

---

## 1. Prometheus metrics: choose the type by the question

```go
// internal/observability/metrics.go
TransfersTotal = promauto.NewCounterVec(           // "how many, by outcome?"
    prometheus.CounterOpts{...},
    []string{"status"},                             // success | failed | wal_append_error | ...

TransferDuration = prometheus.NewHistogram(        // "how long, in distribution?"
    prometheus.HistogramOpts{
        Buckets: prometheus.ExponentialBuckets(0.00005, 2, 16), // 50µs → ~3.2s
    })

RingBufferDepth = prometheus.NewGauge(             // "what's the current level?"
    ...)
```

- **Counter** — only goes up; rate over time is the signal
  (`rate(aequitas_engine_transfers_total[5m])`). Label by *outcome*, not by
  account ID — label cardinality is the classic Prometheus foot-gun (one
  time series per unique label set, forever).
- **Histogram** — bucketed distribution; p50/p95/p99 computable
  server-side. Buckets must bracket reality: `ExponentialBuckets(0.00005, 2,
  16)` spans 50 µs to ~3.2 s, matching what a batch cycle can cost. Buckets
  entirely below your real latency make every sample land in the top bucket
  and percentiles become "≥ last bucket."
- **Gauge** — a current level. The lesson embedded here: `RingBufferDepth`
  was declared and **never set anywhere** — a metric that silently reads 0.
  Alerts computed from it would be lies. The rule: a metric's *first* commit
  includes the code path that updates it (the loop should call
  `RingBufferDepth.Set(float64(l.rb.Len()))` per batch — C0.10), or it
  doesn't ship.

`promauto` registers with the default registry at declaration time — no
wiring step to forget, but also no scoped registries for tests; the
observability test asserts the `/metrics` endpoint serves, which is the
integration point that matters.

## 2. Structured logging with slog

```go
// internal/observability/logger.go
logger := observability.InitLogger(cfg.LogLevel, cfg.LogFormat)  // *slog.Logger
logger.Error("failed to open wal", "error", err)
logger.Info("Node running in PRIMARY mode", "replication_port", cfg.ReplicationPort)
```

`slog` (stdlib since Go 1.21) replaces `fmt.Println`/`log.Printf` with
key-value pairs: machine-parseable when `LogFormat=json`, human-readable as
text. The conventions here:

- Lowercase static message; **details as attributes**, never formatted into
  the message — `"failed to open wal", "error", err`, not
  `fmt.Sprintf("failed to open wal: %v", err)`.
- Levels used as severities: `Error` = operator action needed, `Info` =
  lifecycle milestones, `Debug` = per-request detail (off by default).
- Never log secrets or full financial payloads at Info.

## 3. Configuration: env vars, defaults, and the fail-fast principle

```go
// internal/config/config.go
WALSegmentSize: getEnvInt64("WAL_SEGMENT_SIZE", 64<<20),
Engine:         engine.DefaultConfig(),

cfg.Engine.MaxBatchSize = getEnvInt("MAX_BATCH_SIZE", cfg.Engine.MaxBatchSize)
```

Every knob has a default equal to the in-code default (`getEnvInt` falls
through on parse failure — silent, which is a known P5.4 gap: a typo'd
`MAX_BATCH_SIZE=ten` boots with defaults instead of failing loudly). The
design intent worth copying: **one place defines the defaults
(`engine.DefaultConfig()`), env overrides layer on top, and the config
struct is a plain value** — no global state, no magic singletons, easy to
construct in tests with three fields overridden.

The missing discipline (roadmap P5.4): validate at startup —
`WAL_SEGMENT_SIZE=0`, `RING_BUFFER_SIZE=63` (not a power of two: the ring
buffer's mask math silently breaks) should refuse to boot with a clear
message. Config errors found at 3 a.m. should be text in the log, not
undefined behavior in the queue.

## 4. Lifecycle: the startup and shutdown choreography

`cmd/server/main.go` is worth reading top to bottom as a template. Startup
order — each step can only succeed after the previous:

```
config → logger → WAL open → engine init + RECOVERY → replication role →
observability → REST → gRPC → wait for signal
```

Recovery happens **before any listener opens**: the process is never
network-reachable with unrecovered state. A server that starts serving and
then replays the WAL serves stale balances for the replay window — a
correctness bug that looks like a performance bug. (The roadmap's P5.9
formalizes this as readiness: recovered +, for a follower, caught-up.)

Shutdown is the reverse with the drain discipline from document 02:

```go
<-ctx.Done()                // SIGTERM via signal.NotifyContext
grpcServer.GracefulStop()   // finish in-flight RPCs, refuse new ones
httpServer.Shutdown(ctx5s)  // bounded drain — always give HTTP a deadline
ledger.Close()              // cancel loop → wg.Wait() → WAL fsync+close
```

Note the three different stop styles by component type: gRPC's
`GracefulStop` (no built-in deadline — callers can hang it, which is why
it runs *first* while the process still has time budget), HTTP's
`Shutdown` with an explicit `context.WithTimeout`, and the engine's
cancel-and-wait. Every goroutine this process starts is owned by a struct
with a `Stop`/`Close` that waits (`wg.Wait()`); `main`'s defers run them in
reverse registration order.

## 5. What a principal adds to this layer

The gap between this and production-grade observability (roadmap P5.6,
Phase 18) is well-defined, and each item is a good standalone exercise:

- **Metrics that answer questions:** follower lag (`lastLSN` gap), segment
  count, recovery duration, snapshot age. Current metrics say "how fast";
  these say "how healthy."
- **A never-fire alert** — `invariant_violations_total > 0` — wired to the
  C0.7 policy. The most important alert in a ledger is one that must never
  fire.
- **Trace spans** (OpenTelemetry) across gRPC → engine → WAL sync, so a slow
  p99 has a story, not just a number.

> **Exercise 1.** Set `RingBufferDepth` in the loop's `processBatch` and
> write an integration test asserting the gauge is eventually > 0 under
> load (scrape `/metrics` from a test server).
>
> **Exercise 2.** Add config validation that rejects `RING_BUFFER_SIZE`
> values that are not powers of two, with a test per rejection path. Feel
> how much better "refuses to boot" is than "silently misbehaves."
>
> **Exercise 3.** Add a `/readyz` endpoint that returns 503 until recovery
> has completed and (for followers) `LastLSN` is within N of the primary —
> then wire it into the k8s manifest.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Metric type by question | `metrics.go` | Counter=rate, Histogram=distribution, Gauge=level |
| Label cardinality | `TransfersTotal{status}` | Label by outcome class, never by entity |
| Unset gauge = lying metric | `RingBufferDepth` | Ship the updater with the metric |
| slog attributes | `logger.Info("msg", "k", v)` | Details as fields, messages as constants |
| Config defaults + validation | `config.Load` | Defaults in code; overrides from env; validate or fail |
| Recovery before listening | `main.go` | Never serve unrecovered state |
| Stop style per component | GracefulStop / Shutdown / cancel-wait | Drain intake first; deadline everything; wait for owners |
