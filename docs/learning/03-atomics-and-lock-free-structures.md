# 03 — Atomics & the Lock-Free Ring Buffer

**File in focus:** `internal/engine/ring_buffer.go` (86 lines)

This is the only lock-free structure in the codebase, and it is small enough
to understand line by line. It is a Disruptor-style MPMC (multi-producer,
multi-consumer-safe) submission queue: N API goroutines push transfer events;
the single event-loop goroutine drains them in batches.

---

## 1. The structure

```go
type RingBuffer struct {
    buf   []TransferEvent   // the slots
    ready []atomic.Uint64   // per-slot publish sequence
    mask  uint64            // cap - 1; turns modulo into an AND

    head atomic.Uint64      // producer claim sequence
    tail uint64            // consumer-owned sequence

    tailShared atomic.Uint64 // producer-visible tail snapshot
}
```

Three words coordinate everything. Producers race on `head`; the consumer
owns `tail` outright (no atomic needed for its own reads/writes) and
publishes it to producers via `tailShared`. `mask = cap - 1` works because
the capacity is a power of two — `h & mask` is `h % cap` in one instruction.

Slot buffers (`buf`, `ready`) are preallocated once at construction. The
queue never allocates at runtime — no growth, no GC churn, backpressure by
design instead: a full ring returns `ErrBufferFull` and the caller retries or
fails.

## 2. The CAS claim loop

```go
func (rb *RingBuffer) Submit(ev TransferEvent) error {
    for {
        h := rb.head.Load()
        t := rb.tailShared.Load()
        if h-t >= rb.cap {
            return core.ErrBufferFull{}       // full: no slot to claim
        }
        if rb.head.CompareAndSwap(h, h+1) {
            idx := h & rb.mask
            rb.buf[idx] = ev                  // 1. write payload
            rb.ready[idx].Store(h + 1)        // 2. publish
            return nil
        }
        runtime.Gosched()                     // lost the race; retry
    }
}
```

`CompareAndSwap(h, h+1)` writes `h+1` **only if the current value is still
`h`**. Every producer loads the same `h`, and exactly one CAS wins; the
increment hands out unique slot claims without a lock. Losers reload and
retry (a classic CAS loop — Go's `atomic` package gives no ABA protection,
but none is needed: the sequence only ever increases, and capacity checks use
the monotone difference `h - t`).

The two-step publish is the heart of the memory-model lesson:

1. `rb.buf[idx] = ev` — an ordinary write of a 120-byte struct.
2. `rb.ready[idx].Store(h + 1)` — an atomic store that *publishes* it.

The consumer will only read `buf[idx]` after seeing `ready[idx] == tail+1`.
Go's memory model (Go 1.19+ memory-model document, implemented by
`sync/atomic`) guarantees: an atomic store followed by an atomic load that
observes the stored value creates a **happens-before** edge. Everything the
producer did before the store — including the plain write of `ev` into the
slot — is visible to the consumer after its load. Without the atomic
publish, the consumer could observe the slot's *new* `ready` value but
*stale* bytes in `buf` — a torn event. This is the exact same
write-then-publish pattern as a mutex-protected write, just expressed with
one atomic instead of a lock.

## 3. The consumer side

```go
func (rb *RingBuffer) DrainBatch(max int) []TransferEvent {
    head := rb.head.Load()
    if head == rb.tail { return nil }

    pending := head - rb.tail
    if uint64(max) > pending { max = int(pending) }   // size to what's there (C0.15)
    out := make([]TransferEvent, 0, max)

    for len(out) < max && rb.tail < head {
        idx := rb.tail & rb.mask
        wantReady := rb.tail + 1
        if rb.ready[idx].Load() != wantReady {
            break                                     // producer mid-publish; take what we have
        }
        out = append(out, rb.buf[idx])
        rb.tail++                                     // consumer-owned: plain writes
    }
    rb.tailShared.Store(rb.tail)                      // publish progress to producers
    return out
}
```

Note the division of labor: the consumer's own `tail` needs no atomics —
**single-writer ownership again** (document 02). Only the handoff points are
atomic: producers read `tailShared`, the consumer writes it; producers write
`head`/`ready`, the consumer reads them.

The `ready` check also handles a subtle race: a producer may have claimed a
slot (incremented `head`) but not yet published (`ready` not yet updated)
when the consumer drains. The consumer breaks — it takes the contiguous
prefix of published events, never a torn one, and the straggler is picked up
on the next drain.

`Len()` is deliberately an approximation:

```go
func (rb *RingBuffer) Len() int {
    head := rb.head.Load()
    tail := rb.tailShared.Load()
    if head < tail { return 0 }
    return int(head - tail)
}
```

Between the two loads, both can move. `head < tail` guards the skew where
`tailShared` is *newer* than `head` — it can momentarily read negative. For a
scheduling hint (`controlPending()` in the batcher, a depth gauge), an
approximation is correct; for accounting, it would not be. **Decide
explicitly whether a concurrent value needs to be exact or fresh-enough.**

## 4. Why not just use a channel?

`chan TransferEvent` with a buffer would be half the code. Reasons a database
core reaches for the ring buffer instead:

- **Allocation:** channels are heap-allocated with an internal ring of their
  own; our slots are one preallocated array, touched zero times by GC.
- **Batch draining:** `DrainBatch(max)` takes a contiguous snapshot in one
  pass; draining a channel is per-element receives with lock/rendezvous
  overhead per item.
- **Backpressure semantics:** `Submit` returns a typed error on full; a
  channel send either blocks (kills the API layer's latency) or needs a
  `select`+`default` dance at every call site.
- **Ordering:** CAS-claimed sequence numbers give a strict FIFO by claim
  order — the determinism the WAL replay requires.

The cost is that you now own the memory-model reasoning — which is exactly
what this document is for. Use channels until profiling says otherwise; when
you go lock-free, test like document 07 says (`TestRingBufferConcurrentSubmitNoLossNoDup`
floods 100 producers × 200 events and asserts exactly-once delivery).

## 5. False sharing (what's next for this structure)

CPU caches move whole cache lines (typically 64 bytes). `head` and
`tailShared` are adjacent fields in the struct — likely the *same* cache
line. Producers hammer `head`; the consumer stores `tailShared`; every store
invalidates the other side's cached line. On a fast path touched millions of
times per second, this is measurable.

The standard fix is padding each contended atomic to its own line:

```go
type paddedCounter struct {
    v atomic.Uint64
    _ [56]byte // pad to 64 bytes total
}
```

TigerBeetle goes further and aligns the whole structure to page boundaries.
This is on the Tier-2 list for this repo (S2.1) — measure with
`go test -bench` before and after; on an M-series laptop the delta is small,
on a many-core server it is not.

> **Exercise 1.** Sketch the interleaving where two producers CAS
> simultaneously and one loses. Which values does each goroutine see on
> reload? Prove to yourself that a slot is claimed by exactly one producer.
>
> **Exercise 2.** What happens if a producer crashes between steps 1 and 2
> of publish (claim made, slot written, `ready` not stored)? Trace `DrainBatch`
> and confirm the system stays consistent — then explain why the consumer's
> `break` is what makes this safe.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| CAS loops | `Submit` | Retry on loss; the sequence monotone kills ABA |
| Publish via atomic store | `ready[idx].Store(h+1)` | Plain writes before the atomic are visible after the atomic is observed |
| Single-writer fields | `tail` | Ownership replaces atomics; atomic only at handoffs |
| Approximate reads | `Len()` | Skew is real; decide exactness per call site |
| Backpressure | `ErrBufferFull` | Bounded structures fail fast; unbounded ones fail late |
| False sharing | `head` vs `tailShared` | Pad contended atomics to cache lines |
