# ADR 001 — Single-writer event loop

**Status:** Accepted  
**Bucket:** 1 — Core write mechanics  
**Affects:** `internal/engine/loop.go`, `ring_buffer.go`, `batch.go`, `event.go`

---

## Context

The naive design for a concurrent ledger assigns one goroutine per gRPC request. Each goroutine competes for per-account mutexes, executes the transfer, writes to the WAL, calls `fsync`, and returns. This is the architecture most engineers produce by default.

It has two compounding problems at scale:

**Lock contention.** Any "hot account" — an exchange deposit address, a liquidity pool, a treasury — becomes a serialisation point. Under 100 concurrent goroutines hitting the same account, all 99 waiters block. The throughput of the entire system is bounded by the throughput of the most-contested account.

**Per-transfer fsync cost.** NVMe drives handle roughly 100k–500k random IOPS. If each transfer calls `fsync` independently, that is your ceiling regardless of CPU speed, memory bandwidth, or goroutine count. At 50k TPS you are already saturating a fast NVMe drive with nothing but fsync calls.

These are not problems you can tune away. They are structural.

---

## Decision

Adopt a **single-writer event loop** that is the only goroutine allowed to mutate ledger state.

- All gRPC handlers are **producers** only. They wrap a transfer request in a `TransferEvent` (which carries a `chan error` callback) and push it into a ring buffer.
- The single-writer loop is the **sole consumer**. It drains a batch from the ring buffer, validates, commits to WAL as one group, applies mutations, and signals each callback channel.
- Because only one goroutine mutates `AccountManager`, no mutexes are needed anywhere in the state machine.

### The event envelope

```go
// internal/engine/event.go

type TransferEvent struct {
    Transfer core.Transfer
    Result   chan error  // gRPC handler blocks on this after enqueue
}
```

The gRPC handler does:
1. Build `TransferEvent{Transfer: req, Result: make(chan error, 1)}`
2. Call `ringBuffer.Submit(event)` — returns immediately or `ErrBufferFull`
3. Block on `<-event.Result`
4. Return the result to the client

### The write loop skeleton

```
loop.go → Run():
  for {
      batch := batcher.Collect()          // drain up to N or wait up to T
      wal.AppendBatch(batch)              // serialise all records
      wal.Sync()                          // ONE fsync for the entire batch
      accounts.ApplyBatch(batch)          // pure mutations, no locks
      for _, e := range batch {
          e.Result <- outcome[e]          // unblock each gRPC handler
      }
  }
```

---

## Tradeoffs

### Gain: lock-free throughput

No mutex contention anywhere in the hot path. A hot account that would have serialised 100 goroutines now simply occupies one slot in the batch. The entire batch of 10,000 transfers — including 500 to the same hot account — completes in the time of one `fsync`.

### Gain: group commit amortisation

Because all transfers in a batch share a single `fsync`, the per-transfer I/O cost is `(fsync latency) / (batch size)`. At a batch size of 10,000 and an NVMe `fsync` latency of 100µs, the amortised I/O cost per transfer is 10 nanoseconds — three orders of magnitude cheaper than per-transfer fsync.

### Cost: tail latency under bursty load

If the disk stalls — a delayed `fsync` due to a firmware flush or write buffer saturation — **every gRPC handler in the current batch waits together**. In a per-mutex design, only the goroutines touching that specific account would stall. Here, the entire system pauses as a unit.

**Mitigation:** Expose `batch_timeout_ms` as a configurable parameter. Under low load, the batcher flushes immediately (0-wait drain) to keep latency low. Under high load, it accumulates a full batch. Monitor `wal_sync_duration_seconds` in Prometheus; alert if p99 exceeds your SLA.

### Cost: no horizontal write scaling

A single writer is a single CPU core. You cannot scale writes by adding goroutines. Vertical scaling (faster single-core CPU, faster NVMe) is the lever.

**Mitigation for portfolio purposes:** Document this explicitly. A multi-partition design (shard accounts across N single-writer loops by `account_id % N`) is the production path. Implementing a 2-partition proof of concept is sufficient to show you understand it.

---

## Alternatives considered

**Per-account mutex with lock ordering (rejected).** This was the initial design. Eliminated because it cannot escape the hot-account contention problem and makes group commit impossible (you would need per-transfer fsync to maintain correctness).

**Optimistic concurrency (CAS on balance version) (deferred).** Reads current balance version, computes new balance, CAS-swaps. Avoids locks but introduces retry loops under contention, which makes latency non-deterministic. Valid for a future optimisation pass after proving single-writer throughput.

**Channel-per-account (rejected).** Routes each transfer to the owning account's goroutine via a dedicated channel. Eliminates cross-account lock contention but reintroduces per-transfer I/O (each goroutine still needs to fsync). Complexity without the group-commit benefit.

---

## Key files

| File | Role |
|---|---|
| `internal/engine/loop.go` | The single goroutine — `Run(ctx)`, owns the batch cycle |
| `internal/engine/ring_buffer.go` | `Submit(event)`, `DrainBatch(max int)`, `Len()` |
| `internal/engine/batch.go` | `Collect()` — drain up to N or wait up to timeout |
| `internal/engine/event.go` | `TransferEvent` struct |
| `internal/engine/accounts.go` | `ApplyBatch([]TransferEvent)` — pure value mutations |

---

## Benchmark targets

| Scenario | Target | How measured |
|---|---|---|
| Sequential transfers (1 goroutine) | > 50k TPS | `bench/scenarios.go: SequentialTransfers` |
| Concurrent transfers (100 goroutines) | > 200k TPS | `bench/scenarios.go: ConcurrentTransfers` |
| Hot-account contention (100 → 1 account) | > 150k TPS | `bench/scenarios.go: HighContentionHotAccount` |
| vs naive Postgres baseline | > 10× speedup | `bench/report.go: CompareStores` |
| p99 latency under sustained load | < 5ms | Prometheus histogram, `bench/harness.go` |
