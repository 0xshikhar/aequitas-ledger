# ADR 005 — Idempotency store with LRU + TTL eviction

**Status:** Accepted  
**Bucket:** 2 — Operational realities  
**Affects:** `internal/engine/idempotency.go`, `internal/engine/event.go`

---

## Context

Idempotency keys prevent double-charging when a client retries a failed or timed-out request. The pattern is:

1. Client sends `CreateTransfer` with `Idempotency-Key: abc-123`
2. Server processes the transfer and stores `{key: "abc-123", result: Transfer{...}}`
3. Client's network times out — it doesn't know if the request succeeded
4. Client retries with the same key
5. Server finds `"abc-123"` in the store and returns the cached result without re-executing

Without idempotency keys, a retry after a network timeout causes a double-debit — one of the most serious bugs in a financial system.

### The unbounded memory problem

At 200k TPS, the ledger processes 200,000 transfers per second. If every transfer has an idempotency key and keys are retained forever:

```
memory per key ≈ 64 bytes (key string) + 128 bytes (Transfer struct) ≈ 200 bytes
keys after 1 hour ≈ 200,000 × 3,600 = 720,000,000 keys
memory after 1 hour ≈ 720M × 200 bytes = 144 GB
```

An in-memory idempotency store that never evicts will exhaust RAM in under an hour at target throughput. This is not a theoretical concern — it is the most common "works in testing, explodes in production" failure mode for idempotency implementations.

---

## Decision

Implement the idempotency store as a **bounded LRU cache with TTL-based expiry**:

- **Maximum entries:** Configurable, default 5,000,000 (approximately 1GB at 200 bytes per entry)
- **TTL:** Configurable, default 24 hours — matches typical client retry window
- **Eviction policy:** LRU (least recently used) when the size limit is reached before TTL expiry

### Eviction tradeoffs

**TTL expiry** removes keys after a fixed time window. If a client retries after the TTL, the server will re-execute the transfer. This is acceptable if:
- The TTL is longer than the client's maximum retry window
- The client is responsible for not retrying after TTL expiry

**LRU eviction** under memory pressure removes keys that have not been looked up recently. A key evicted by LRU could theoretically be retried by a client, causing a re-execution. This is the correct tradeoff because:
- LRU eviction only occurs when the store is at capacity (5M entries = approximately 1GB)
- In practice, retries happen within seconds or minutes, while LRU eviction targets keys unused for hours
- The alternative (unbounded growth) is guaranteed to crash the server

### idempotency.go API

```go
// internal/engine/idempotency.go

type IdempotencyStore struct {
    maxSize int
    ttl     time.Duration
    entries lruMap    // doubly-linked list + hash map, value type entries
    mu      sync.RWMutex  // NOTE: this is the ONE mutex allowed outside the engine
}

// CheckAndReserve atomically checks for an existing result or reserves the key.
// Returns:
//   - (existing, true, nil)  — key already committed, return cached result
//   - (zero, false, nil)     — key reserved, caller must call Commit or Rollback
//   - (zero, false, err)     — key already reserved by another goroutine (in-flight)
func (s *IdempotencyStore) CheckAndReserve(ctx context.Context, key string) (core.Transfer, bool, error)

// Commit persists the result for a key after successful transfer execution.
func (s *IdempotencyStore) Commit(key string, result core.Transfer) error

// Rollback releases a reserved key after a failed transfer.
// The next request with the same key will be retried.
func (s *IdempotencyStore) Rollback(key string)

// Evict removes all entries past their TTL. Called by a background goroutine.
func (s *IdempotencyStore) Evict()
```

### Why the idempotency store has its own mutex

The idempotency check happens in the **gRPC handler** (the API layer), not inside the single-writer loop. The gRPC handler runs concurrently on many goroutines. Therefore the idempotency store is the one component in the system that is accessed by multiple concurrent goroutines and needs a mutex.

This is explicitly not a violation of the single-writer architecture — the idempotency store is a read-mostly cache (the common path is a cache hit, which is a read). The write (Commit) only happens once per unique key, after the single-writer loop has finished processing the transfer.

The sequence is:

```
gRPC handler (concurrent):
  1. CheckAndReserve(key) — acquires RLock for check, upgrades to Lock only on miss
  2. If hit: return cached result to client immediately
  3. If miss: enqueue TransferEvent to ring buffer, block on Result channel

Single-writer loop:
  4. Processes transfer
  5. Sends result down Result channel

gRPC handler (resumes after step 3):
  6. Calls Commit(key, result) — acquires Lock, writes to LRU cache
  7. Returns result to client
```

### WAL interaction

Idempotency keys are **not** written to the WAL in v1. On restart, the idempotency store is empty. This means:

- Retries within the restart window will be re-executed
- The re-execution is safe because the WAL replay already applied the original transfer — the state machine's `ValidateTransfer` will return `ErrInsufficientFunds` or `ErrDuplicateTransferID` if the transfer ID is already in the account history

In a production system, idempotency keys would be written to a persistent store (separate from the main WAL, to avoid write amplification). This is a Bucket 2 extension deferred to v2.

---

## LRU implementation

Use a doubly-linked list + map structure (standard LRU pattern). Do **not** use `container/list` from the standard library — it uses `interface{}` and heap-allocates every element. Implement a typed, value-based LRU to avoid GC pressure:

```go
// internal/engine/idempotency.go (implementation detail)

type entry struct {
    key       string
    value     core.Transfer
    status    entryStatus // pending, committed, rolledback
    expiresAt time.Time
    prev, next *entry
}

type entryStatus uint8
const (
    statusPending   entryStatus = 0
    statusCommitted entryStatus = 1
)
```

The `prev` and `next` pointers mean `entry` is heap-allocated — this is acceptable because idempotency entries are long-lived (24h TTL) and therefore GC scan cost is amortised over many seconds, not per-transfer.

---

## Configuration

```go
// internal/config/config.go
type IdempotencyConfig struct {
    MaxEntries int           // default: 5_000_000
    TTL        time.Duration // default: 24h
    EvictInterval time.Duration // how often background eviction runs, default: 1m
}
```

Expose as environment variables:
- `LEDGER_IDEMPOTENCY_MAX_ENTRIES`
- `LEDGER_IDEMPOTENCY_TTL_HOURS`

---

## Observability

Add to `internal/observability/metrics.go`:

```
ledger_idempotency_hits_total          # counter — key found in cache
ledger_idempotency_misses_total        # counter — key not found, transfer executed
ledger_idempotency_evictions_total     # counter — entries removed by LRU or TTL
ledger_idempotency_store_size          # gauge — current entry count
```

A high `evictions_total / hits_total` ratio indicates the TTL or max-size is too small for the retry window of connected clients.

---

## Alternatives considered

**No eviction (rejected).** Unbounded memory growth. Server crashes in production within hours at target TPS.

**Redis-backed idempotency store (deferred to v2).** Solves the memory problem and survives restarts. Adds a network round-trip to the hot path (gRPC handler must call Redis before enqueuing to the ring buffer). In v1, the in-process LRU is simpler and faster.

**Bloom filter for duplicate detection (rejected for primary path).** A Bloom filter can quickly answer "definitely not seen" vs "probably seen," but it cannot return the cached result — only detect that the key exists. Useful as a pre-filter to reduce LRU lookups but not a replacement for the store itself.
