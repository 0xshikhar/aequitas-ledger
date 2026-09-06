# 08 — Benchmarking & Performance Engineering

**Files in focus:** `tests/unit/engine_alloc_bench_test.go`,
`bench/harness.go`, `internal/observability/pprof.go`, and the measured
before/after numbers from the C0.15/C0.16 work.

Performance work is a measurement discipline. This document teaches the
methodology through this repo's real numbers: a benchmark that lied, a
1.2 MB allocation found by refusing to trust it, and percentile math done
twice (wrongly and rightly).

---

## 1. The benchmark that lied

The first version of this project's transfer benchmark reported:

```
BenchmarkLedgerCreateTransferAllocs   53.39 ns/op   0 B/op   0 allocs/op
```

Fifty-three nanoseconds for a durable, fsynced financial transfer is not a
good result — it is an impossible one. The benchmark was broken: every
iteration reused the **zero idempotency key**, so after the first call every
subsequent call was a dedup cache hit that never reached the WAL. The fix
was in the harness, not the engine:

```go
putUint64(key[:8], uint64(i+1)) // unique idempotency key, else every op after the first is a dedup cache hit
```

Rule one of benchmarking: **a result you cannot explain is a result you
cannot trust.** Sanity-check against physics — a single fsync costs
microseconds to milliseconds; anything below that means you're not measuring
the path you think you are.

The corrected baseline told the real story:

```
BenchmarkLedgerCreateTransferAllocs   5436478 ns/op   1204884 B/op   9 allocs/op
```

## 2. Reading the numbers: ns/op, B/op, allocs/op

- **ns/op** includes waiting. This benchmark submits synchronously, so most
  of the 5.4 ms is the batcher's 1 ms timeout + fsync latency — wall-clock
  per op, not CPU per op.
- **B/op and allocs/op** count heap allocations on the whole path. These are
  the numbers you can *attribute*: 9 allocations per transfer is not
  mysterious — you can name each one.
- **The dominant term was not a mystery either:** 1,204,884 B/op ≈ 10,000
  events × ~120 bytes. One grep later: `DrainBatch` preallocated
  `make([]TransferEvent, 0, maxBatchSize)` regardless of how many events
  were pending (document 01, §4).

The fix and the re-measure:

```
BEFORE:  1204884 B/op   9 allocs/op     (channel alloc + 1.2 MB slice + encoding)
AFTER:       656 B/op   7 allocs/op     (encoding only; channel pooled)
```

That is the whole cycle: **measure → sanity-check → attribute → fix →
re-measure.** Never the reverse order, and never skip the re-measure (the
first "fix" moved B/op by only 112 bytes until the slice was also fixed —
without re-measuring we'd have claimed victory over the wrong line).

## 3. Benchmark hygiene in this repo

```go
// tests/unit/engine_alloc_bench_test.go
func BenchmarkLedgerGetAccountAllocs(b *testing.B) {
    l := setupAllocBenchLedger(b)      // TempDir + Cleanup via b.Cleanup
    ...
    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ { ... }
}
```

- `b.ReportAllocs()` in-code beats `-benchmem` on the CLI: benchmarks that
  care about allocation always report it.
- `b.ResetTimer()` after setup; `b.StopTimer()/StartTimer()` around
  per-iteration setup you cannot hoist — **but allocations during the
  stopped window still count in B/op**, which is why
  `BenchmarkWALTruncateBefore` uses a rising cut LSN in a single pass
  instead of re-copying a WAL per iteration.
- `b.TempDir()` + `b.Cleanup(func(){...})` for teardown — no leaked
  directories between runs.
- Vary `-benchtime` (e.g. `-benchtime=2s` for stable numbers,
  `-benchtime=100x` for quick loops) and run benchmarks several times;
  laptop benchmarks vary ±20% run to run. `benchstat` over multiple runs is
  the honest comparison tool.

## 4. Percentiles done right — twice

The bench harness (`bench/harness.go`) collects per-op latencies into
per-worker slices, then merges and sorts once at the end:

```go
allLat = append(allLat, workerLat...)
sort.Slice(allLat, func(i, j int) bool { return allLat[i] < allLat[j] })
p50 = allLat[n*50/100]
```

Sorting at the end instead of maintaining a sorted structure per op is the
right call — O(n log n) once. The subtler lesson is in the *latency test*
(document 07): the same math, but with a documented budget and a load
pattern that reproduces the failure regime. A percentile without a budget is
a dashboard; a percentile against a budget is a test.

Where do percentiles lie? When the sample is small (p99 of 50 samples is the
max in disguise), when the load is artificial (2 synchronous writers never
starved anyone; 16 continuous writers did), and when the tail is dominated
by a shared cause (fsync) that no amount of Go code can remove — which is
why the budget is 10 ms and not 1 ms.

## 5. Profiling: finding where the time and memory go

The observability server exposes pprof on its own port:

```go
// internal/observability/pprof.go — /debug/pprof/ on :6060
import _ "net/http/pprof"
```

The workflow against a running server:

```sh
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=15   # CPU
go tool pprof http://localhost:6060/debug/pprof/heap                 # live heap
go tool pprof http://localhost:6060/debug/pprof/allocs               # cumulative allocations
```

In `pprof`, the commands that matter: `top` (hottest functions), `list
FuncName` (line-level attribution — this is how you find a single
`make()` line eating 90% of B/op), `web` (flame graph). For benchmarks,
`-cpuprofile`/`-memprofile` flags feed the same tooling without a server.

Escape analysis is the theory behind allocation findings: `go build
-gcflags='-m'` prints decisions like "escapes to heap." Value structs that
stay on the stack cost nothing at allocation; anything retained past the
function (into a channel, a slice that outlives the call, an interface) is
heap-allocated. The one-shot channel pool (C0.15) exists because a channel
sent into a queue necessarily escapes.

## 6. The economics of batching

The deepest performance insight in this codebase is not in Go at all — it is
arithmetic. A group commit serves B transfers per fsync. Cost model:

```
throughput ≈ B / (fsync_latency + cpu_per_batch)
read p99   ≈ batch_wait + fsync_latency   (the fair loop's bound)
```

Raising B is the only lever that improves *both* terms — which is why the
batched API (D1.1) is the roadmap's biggest TPS lever, why the read-latency
budget is generous at p99, and why the future load generator (Phase 17) must
drive *batchable* load to say anything honest. Unary request/response at
concurrency C gives you B ≈ C at best; a client that submits 8,192 transfers
per RPC gives you B = 8,192 regardless of concurrency.

Corollary: idle CPU is a correctness-adjacent performance issue. The loop
spins (document 02, §5); at 100% of one core while doing nothing, a
production deployment reads that as a runaway process. "Remove the spin"
(S2.1) is on the roadmap with an acceptance test: idle CPU < 1%.

> **Exercise 1.** Break `DrainBatch` back to `make([]TransferEvent, 0, max)`
> and run `BenchmarkLedgerCreateTransferAllocs`. Explain the exact B/op you
> see from first principles (sizeof(TransferEvent) × maxBatchSize).
>
> **Exercise 2.** Capture a CPU profile of the read-latency test and find
> where p50 latency actually goes: batcher spin, channel handoff, or WAL
> sync? Change `BatchTimeout` to 100µs and re-measure — what improved, what
> got worse?
>
> **Exercise 3.** Run `go build -gcflags='-m' ./internal/engine/...` and
> find one function where a value could stay on the stack but escapes.
> Explain what forces the escape.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Unexplainable results | 53 ns transfer | If physics disagrees, the harness is broken |
| Attribute before fixing | 1.2 MB ≈ 10k × 120 | Name the allocation before deleting it |
| Re-measure after every change | C0.15 cycle | Fixes that don't move numbers are hypotheses |
| ReportAllocs + ResetTimer | all benchmarks | Hygiene or the numbers are noise |
| Percentiles need budgets | read-latency test | Load pattern must reproduce the failure regime |
| pprof + `-gcflags=-m` | observability server | `list FuncName` for line-level truth |
| Batch arithmetic | group commit | B is the lever that improves every metric at once |
