# 02 — The Single-Writer Concurrency Model

**Files in focus:** `internal/engine/loop.go`, `internal/engine/ledger.go`,
`internal/engine/batch.go`, `internal/engine/event.go`,
`cmd/server/main.go`

This is the architectural heart of the project, and its most important Go
lesson: **the cheapest concurrency bug is the one that cannot exist.** One
goroutine owns all mutable financial state. Everything else — reads, writes,
account creation — talks to it by message.

---

## 1. Why one goroutine owns the state

`AccountManager` has zero synchronization primitives. Every field in it is
plain, unsynchronized memory. That is safe because of an ownership rule:

> After `NewLedger` finishes construction, only the event-loop goroutine may
> call methods on `AccountManager`. Everyone else sends messages.

The payoff, compared to a `sync.RWMutex`-protected map:

- **No lock contention.** 200 API goroutines don't fight over one mutex.
- **Deterministic ordering.** Transfers apply in exactly the order they enter
  the ring buffer; the WAL replay reproduces it bit-for-bit. With
  fine-grained locking, ordering depends on lock-acquisition timing — chaos
  to replay.
- **Batch atomicity for free.** The loop validates the whole batch, then
  applies it, with no other observer able to see a half-applied state.
- **Invariant reasoning.** `SUM(debits) == SUM(credits)` is provable because
  no mutation ever interleaves with a check.

This is the TigerBeetle architecture, and it is also how the Go runtime
scheduler itself works: one goroutine owns a resource, and the rest
communicate. *Don't communicate by sharing memory; share memory by
communicating.*

## 2. The event loop, before and after

```go
// internal/engine/loop.go (current)
func (l *EventLoop) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }

        l.drainControl()
        batch := l.batcher.Collect(l.controlPending)
        l.drainControl()

        if len(batch) == 0 {
            continue
        }
        l.processBatch(batch)
    }
}
```

The old version had a `select { case req := <-l.readQueue: ...; default: }`
followed by an unconditional `l.batcher.Collect()`. Understand why that
starved reads (C0.16), because it is the deepest `select` lesson in the repo:

- `select` with a `default:` branch **never blocks**: if no case is ready, it
  runs `default`. So the drain section picks up *at most one* queued read per
  loop iteration — and only if one is waiting at that exact instant.
- `Batcher.Collect()` spins (see §5) until it has `MaxBatchSize` events or a
  1 ms timeout. Under continuous write load the ring buffer is never empty,
  so `Collect` returns instantly, over and over.
- Net effect: the loop iterates at maximum speed doing write work; the one
  read drained per iteration becomes a fixed trickle. With N readers queued,
  reader latency grows as N × batch-cycle-time. Unbounded.

The fix is the `yield` callback: `Collect` consults
`l.controlPending()` (are any queues non-empty?) on every spin iteration and
returns early if so, plus `drainControl` empties *all* pending control work
before and after each batch. Measured result in
`tests/integration/read_latency_test.go`: **p50 1 ms → 21.9 µs** under 16
concurrent writers.

> **Exercise 1.** Check out the old shape, run `TestReadLatencyBoundedUnderWriteLoad`
> with `Collect(yield)` replaced by `Collect(nil)`, and watch it fail. Then
> make it fail *worse* by adding more writers. Starvation is load-dependent —
> a test with one writer does not catch it. (That is why the test uses 16.)

## 3. The one-shot channel pattern — and pooling it

Every request needs exactly one reply. The idiomatic shape:

```go
// internal/engine/event.go
var readResultPool = sync.Pool{New: func() any { return make(chan ReadAccountResult, 1) }}

func (l *Ledger) GetAccount(ctx context.Context, id [16]byte) (core.Account, error) {
    resChan := AcquireReadResult()
    ev := ReadAccountEvent{ID: id, Result: resChan}

    select {
    case l.loop.readQueue <- ev:       // enqueue
    case <-ctx.Done():
        ReleaseReadResult(resChan)     // never queued; safe to recycle
        return core.Account{}, ctx.Err()
    }

    select {
    case res := <-resChan:             // reply arrived
        ReleaseReadResult(resChan)     // drained; safe to recycle
        return res.Account, res.Err
    case <-ctx.Done():
        // ABANDONED: the loop may still send later — do NOT recycle.
        return core.Account{}, ctx.Err()
    }
}
```

Four things to internalize:

1. **Buffered channel of capacity 1 as a future.** The sender never blocks
   (`req.Result <- res` always succeeds immediately), so the event loop cannot
   stall on a slow caller. Unbuffered would couple the writer goroutine's
   speed to the reader's.
2. **`sync.Pool` for channels.** Allocation is not free at 200k requests/sec.
   The pool recycles channels with a strict rule stated in `event.go`'s
   comment: recycle only after the reply was received, or when the event was
   never queued. Recycle a channel that later receives a stale reply, and the
   *next* requester reads the previous request's answer — a cross-request
   data leak, the worst kind of bug. Note the asymmetry: the ctx-cancelled
   branch after enqueue deliberately does **not** release.
3. **Two selects, two phases.** Phase 1 races enqueue vs cancellation
   (backpressure: a full `readQueue` blocks here). Phase 2 races reply vs
   cancellation. The cancellation path is not an error path — it is the
   *normal* path for a client timing out, and it must not corrupt shared
   state.
4. **Who closes channels?** Nobody. One-shot channels with a single sender
   and single receiver are garbage-collected without closing. Closing is for
   broadcast; here there is exactly one listener.

## 4. context.Context: cancellation as a first-class value

`context` threads through every API call. The rules this codebase follows:

- **`ctx` is the first parameter**, always named `ctx`.
- **Check `ctx.Done()` where you wait.** Every `select` that waits also
  selects on `<-ctx.Done()`.
- **Context values are for request-scoped metadata**, accessed via a private
  key type so no other package can collide:

  ```go
  // internal/api/idempotency.go
  type idempotencyKeyCtxKey struct{}   // empty struct: zero allocation

  func ContextWithIdempotencyKey(ctx context.Context, key [32]byte) context.Context {
      return context.WithValue(ctx, idempotencyKeyCtxKey{}, key)
  }
  ```

  Using `idempotencyKeyCtxKey{}` (unexported, zero-size) instead of a string
  key makes cross-package value collisions impossible.
- **`context.WithCancel` for lifecycle.** `Ledger` holds `ctx`/`cancel`;
  `Close()` calls `cancel()` and the loop exits at its next select. Cancellation
  is cooperative: the loop checks between iterations, never mid-batch.

## 5. Spinning: `runtime.Gosched` and when it's acceptable

```go
// internal/engine/batch.go
for b.rb.Len() < b.maxBatchSize {
    if b.timeout == 0 || time.Now().After(deadline) { break }
    if yield != nil && yield() { break }
    runtime.Gosched()
}
```

`runtime.Gosched()` yields the scheduler tick: "let another goroutine run,
then resume me." A spin loop trades CPU for latency — no sleep/wake overhead,
sub-microsecond reaction to new work. That is the right trade *while a batch
is forming* (the wait is bounded by `BatchTimeout`, 1 ms). It is the wrong
trade at idle: the loop spins forever when there is no work at all (a known,
deliberate simplification — see S2.1 in the roadmap, which replaces idle spin
with a notify channel + timer). The general rule: **spin when the wait is
short and bounded; block when it might be long.**

## 6. WaitGroup and shutdown ordering

```go
// internal/engine/ledger.go
l.wg.Add(1)
go func() {
    defer l.wg.Done()
    l.loop.Run(l.ctx)
}()

func (l *Ledger) Close() error {
    if l.cancel != nil { l.cancel() }
    l.wg.Wait()
    return l.wal.Close()
}
```

`Add` before `go`, `defer Done` inside the goroutine, `Wait` after `cancel` —
the canonical lifecycle trio. The ordering inside `Close` is the lesson:
**cancel first, wait second, close the WAL last.** Closing the WAL while the
loop might still append is a use-after-close race; `wg.Wait()` proves the
loop is gone before the file handle dies.

`cmd/server/main.go` extends the same discipline to the whole process, and
the *order* is a designed decision:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
<-ctx.Done()                 // SIGTERM arrives
grpcServer.GracefulStop()    // 1. stop admitting new RPCs, finish in-flight
_ = httpServer.Shutdown(...) // 2. drain HTTP with a 5s deadline
_ = ledger.Close()           // 3. cancel loop, wait, fsync + close WAL
```

Writes are drained *before* the WAL closes so no acked transfer is lost to a
shutdown race. `signal.NotifyContext` converts an OS signal into context
cancellation — the standard bridge between the process world and the context
world.

## 7. The race detector as architecture review

Run `go test -race ./...`. The detector uses vector clocks (happens-before
analysis) on the real execution: if two goroutines ever touched the same
memory without a synchronization edge between the accesses, it reports the
two stacks that raced. This project's read path *was* such a bug (C0.1):
API goroutines read `AccountManager` while the loop wrote it. The fix —
route reads through the loop (this document's §3) — is what `-race` plus a
concurrent test would have flagged on day one.

Two things the detector cannot do: prove absence of races (it only sees
executed paths), or catch logical races (two correct-by-memory-model reads
that disagree about *meaning* — that was C0.18, the zero key). The detector
is one layer of a testing strategy, not the whole thing — document 07.

> **Exercise 2.** In a scratch copy, delete the `drainControl` call after
> `Collect` and add a goroutine that floods `GetAccount` while another
> floods transfers. Run with `-race`. Then re-add it and watch the latency
> histogram collapse. (Use `tests/integration/read_latency_test.go` as the
> template.)

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Single-writer ownership | `EventLoop` + `AccountManager` | Confine mutable state to one goroutine; message everyone else |
| `select` + `default` | Old `Run()` — C0.16 | Non-blocking drain serves *at most one* item per iteration; a busy producer starves it |
| One-shot channels + pool | `event.go`, `ledger.go` | Cap-1 channel = future; recycle only when provably drained |
| Context | everywhere | Check `Done()` at every wait; private key types for values |
| `Gosched` spin | `Batcher.Collect` | Spin for short bounded waits; block otherwise |
| Shutdown ordering | `Close`, `main.go` | Stop intake → drain → wait → release resources |
| Race detector | CI gate | Necessary, not sufficient |
