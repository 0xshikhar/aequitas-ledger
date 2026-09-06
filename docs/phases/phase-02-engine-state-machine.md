# Phase 2 — Single-Writer Engine State Machine Deep-Dive

## 1. Objective & Problem Statement

In traditional database architectures (such as PostgreSQL or MySQL), concurrent transfers lock accounts using row-level mutexes or `SELECT FOR UPDATE` locks. Under high concurrency, this causes **lock contention, deadlocks, context switching overhead, and thread CPU starvation**.

**Aequitas Ledger** uses LMAX Disruptor-inspired architecture:
1. A **single-writer event loop** processes all account balance mutations in strict sequence on a dedicated CPU core.
2. Concurrent gRPC request handlers submit incoming transfers asynchronously into a lock-free **Ring Buffer**.
3. A **Batcher** drains incoming transfers from the Ring Buffer, validates balances, appends WAL records in batch, and applies mutations in linear order without a single mutex lock on account state.

---

## 2. Core Go Language Concepts & Engineering Internals

### A. Lock-Free Single-Writer Model
By restricting `AccountManager` state mutations to a single goroutine (the `EventLoop`), we completely eliminate mutex locks (`sync.Mutex` or `sync.RWMutex`) on account balances.

```
gRPC Goroutine 1 ──┐
gRPC Goroutine 2 ──┼──> RingBuffer (Channel) ──> EventLoop (Single Goroutine) ──> Account State
gRPC Goroutine 3 ──┘
```

#### Why Single-Threaded Processing is Fast
Modern CPU cores can execute over 3,000,000,000 instructions per second. When memory accesses are sequential without cache-invalidation contention caused by cross-thread mutexes, a single Go goroutine can validate and apply over **500,000 transfers per second**.

---

### B. Ring Buffer & Batcher Component
The `RingBuffer` acts as a non-blocking queue between concurrent API submitters and the engine loop:

```go
type RingBuffer struct {
	ch chan TransferEvent
}

func (rb *RingBuffer) Submit(ev TransferEvent) error {
	select {
	case rb.ch <- ev:
		return nil
	default:
		return core.ErrBufferFull{}
	}
}
```

* **Go Concept (`select` with `default`)**: Provides non-blocking channel insertion. If the ring buffer is full, it immediately returns `ErrBufferFull` to the client instead of blocking the gRPC handler thread.

The `Batcher` drains the channel efficiently:
```go
func (b *Batcher) Collect() []TransferEvent {
	batch := make([]TransferEvent, 0, b.maxSize)
	select {
	case ev := <-b.rb.ch:
		batch = append(batch, ev)
		// Drain all immediately available events up to maxSize
		for len(batch) < b.maxSize {
			select {
			case ev := <-b.rb.ch:
				batch = append(batch, ev)
			default:
				return batch
			}
		}
	case <-time.After(b.timeout):
	}
	return batch
}
```

---

### C. Bounded LRU/TTL Idempotency Store
Payment systems must prevent duplicate processing of the same request key (e.g. network retries). The `IdempotencyStore` tracks keys using an in-memory hash table paired with a doubly-linked LRU list:

```go
type IdempotencyStore struct {
    maxSize int
    ttl     time.Duration
    mu      sync.Mutex
    entries map[[32]byte]*idempEntry
    lru     *list.List
}
```

* **State Transitions**: `RESERVED` $\rightarrow$ `COMMITTED` (or `ROLLBACK` on validation error). If two concurrent API calls arrive with the same key, the second call receives `ErrIdempotencyConflict` and yields execution (`runtime.Gosched()`) until the first commits or rolls back.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **Concurrency Control** | Single-Writer Event Loop | Fine-grained account `sync.Mutex` | Fine-grained locks cause deadlocks (e.g. Tx A: Acc 1 -> Acc 2 vs Tx B: Acc 2 -> Acc 1) and massive context-switch overhead under high concurrency. |
| **Ingress Queue** | Buffered Channel Ring Buffer | `sync.Cond` / Array Queue | Go channels use runtime lock optimizations and integrate natively with Go's `select` scheduler. |
| **Idempotency** | In-Memory Bounded LRU + TTL | Redis / External KV store | In-memory evaluation avoids network RTT (0.1ms vs 2ms latency per request). |
