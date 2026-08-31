# Performance Benchmark Report: Aequitas Ledger

- **Date:** 2026-09-06 (supersedes the 2026-09-01 report)
- **Platform:** Apple M4 Pro (macOS darwin/arm64), Go 1.25
- **Method:** all numbers below are reproduced by committed code —
  `make bench` / the named benchmarks in `tests/unit` and `tests/integration`.
  No number in this report is unreproducible from the repo.

---

## Executive summary

1. **Throughput is batch-shaped, and that is the headline.** Through the real
   gRPC stack, one-transfer-at-a-time RPCs sustain **~211 TPS**; batched RPCs
   (8,192 transfers per call) sustain **~415k TPS — a ~1,970× ratio**. This is
   not a micro-benchmark artifact: both paths run the full gRPC
   serialization → single-writer engine → group-commit WAL pipeline. The
   arithmetic is structural: one fsync amortized over N transfers, and one
   network round-trip over N transfers.
2. **The write path is allocation-lean.** After S2.1 (single-buffer batch
   encode, one sequential Write per batch, `fdatasync`), the engine submits a
   batch of 8,192 transfers with **~1.0 allocation per transfer** (down from
   7 unary / 3.0 batched before S2.1) — the remaining allocation is the
   idempotency-key map insert, which is the price of deduplication.
3. **The event loop no longer burns CPU at idle** — it blocks on channel
   waits against every real wakeup source (producer arrivals, reads, account
   creates, snapshots, shutdown). Idle CPU ~0%; read latency at idle ~0.6 µs.
4. **Recovery is honest.** Full WAL replay of 163,840 transfers takes
   **~0.5 ms** (warm); snapshot-served restart **~0.7 ms** at this size. The
   snapshot's value is not the constant at this scale — it is the bound:
   full replay is O(total history) forever, snapshot-served recovery is
   O(records since the last snapshot).

## Where the time actually goes (pprof evidence)

Captured profiles live in `docs/learning/assets/d11-pprof-*.txt`.

- **Unary path:** CPU is dominated by goroutine park/unwake
  (`pthread_cond_wait`/`pthread_cond_signal`) — per-RPC overhead, not
  ledger work. The engine is a rounding error.
- **Batch path:** CPU is dominated by `syscall.rawsyscalln` (the fsync) —
  the engine's own orchestration accounts for **0.017%** of allocations
  (`Ledger.CreateTransfers` in the batch alloc profile). After S2.1 the
  remaining per-transfer allocation is the idempotency map insert.

**Conclusion: at batch sizes clients actually use, this engine is
fsync-bound. Further Go-side optimization is noise until the storage layer
changes (direct I/O, preallocation — S2.2).**

## Throughput: unary vs batch

Through the gRPC stack (`tests/integration/grpc_bench_test.go`, bufconn,
sequential RPCs, 300 samples):

| Path | ns/op | What one op is | Throughput |
|---|---|---|---|
| `CreateTransfer` (unary) | 4,771,794 | 1 transfer | **~211 TPS** |
| `CreateTransfers` (batch) | 19,742,205 | 8,192 transfers | **~415,000 TPS** |

**Ratio: ~1,966×.** The README's 200k–500k TPS target is met by the batched
API at batch size 8,192 and is fsync-bound at that point. Unary traffic will
never reach it — by design of the cost model, not by lack of tuning.

## Engine-level batch-size curve

`BenchmarkLedgerCreateTransfersBatch` (engine API directly, unique
idempotency keys, 300 samples):

| Batch size | ns/op | allocs/op | allocs/transfer |
|---|---|---|---|
| 1 | 4,783,543 | 10 | 10.0 |
| 128 | 4,877,014 | 148 | 1.16 |
| 1,024 | 6,341,742 | 1,056 | 1.03 |
| 8,192 | 12,915,983 | 8,293 | **1.01** |

Reading: fixed per-batch costs (batch wait + fsync) amortize fully by
~1,000 items; per-transfer allocation cost is flat at ~1 (idempotency map).
The curve's flatness after 1k is why the API caps batches at 8,192 — beyond
that, clients gain nothing and only increase per-RPC memory.

## Allocation story (S2.1 before/after)

`tests/unit/engine_alloc_bench_test.go`, 200 samples:

| Benchmark | Before S2.1 | After S2.1 |
|---|---|---|
| Unary transfer | 683 B/op, 7 allocs | **476 B/op, 4 allocs** |
| Batch-8192 | 11.0 MB/op, 24,650 allocs (3.01/t) | **8.8 MB/op, 8,293 allocs (1.01/t)** |
| GetAccount | 608 ns, 0 allocs | **617 ns, 0 allocs** (unregressed) |

What S2.1 changed: the WAL encodes the whole batch into one reusable buffer
(`appendFrame` with no per-record allocation) and issues one sequential
`Write` per segment (cutting only at frame boundaries when a rotation splits
a batch); payload codecs gained append-variants (`core.AppendTransferPayload`)
used by the loop's scratch buffers; `Sync` uses `fdatasync` on Linux; the
event loop's busy-spin loops were replaced with channel waits.

## Read latency under write load

`TestReadLatencyBoundedUnderWriteLoad` (16 continuous concurrent writers +
read hammer): **p50 ≈ 22 µs, p99 ≈ 5 ms.** The p99 budget is one batch cycle
(batch wait + non-preemptible fsync) — a fair single-writer loop cannot do
better, and the property the test pins is that reads never starve.

## Recovery time

`TestRecoveryTimeFullReplayVsSnapshot` (163,840 transfers, head LSN 163,863,
min-of-5 warm runs):

| Path | Time |
|---|---|
| Full WAL replay (all 163,863 records) | **~507 µs** |
| Snapshot-served restart (delta above snapshot) | **~678 µs** |

**Honest analysis:** at this history size the snapshot load's fixed cost
(stat/read/verify/decode) roughly equals replaying 163k compact records —
both are sub-millisecond, and the snapshot path was *not* faster. That is
the point of measuring: the snapshot's value is asymptotic, not constant.
Full replay cost is O(every transfer ever) — at this rate, 16.4M records
would cost ~50 ms and grow linearly forever — while snapshot-served recovery
stays at O(records since the last snapshot). The background checkpointer
(C0.5) keeps that bound real in production.

## Caveats — read before quoting any number above

1. **Single node, single writer.** All paths exercise one engine; the
   replication stream is not in these numbers.
2. **fsync-bound.** The absolute TPS is a function of this machine's fsync
   latency. On different hardware, scale expectations by device fsync
   latency, not by the ratios (which are structural).
3. **bufconn, not TCP.** gRPC benchmarks use an in-memory listener; real
   networks add per-RPC latency, which makes batching *more* important,
   not less.
4. **The unary number is the honest baseline.** Any client comparing this
   ledger to a naive per-transaction store should compare against the
   unary row — the batched row is what a well-behaved client achieves.
5. CI's bench smoke (`scripts/bench_smoke.sh`) floors are 3–5× looser than
   these numbers to avoid flakes on shared runners; percentage-level
   regression gating needs dedicated hardware.
