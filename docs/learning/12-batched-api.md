# 12 — The Batched Request/Response Protocol (D1.1)

**Files in focus:** `internal/engine/ledger.go` (`CreateTransfers`,
`CreateAccounts`), `internal/engine/event.go` (`batchState`, `TransferAck`),
`internal/engine/loop.go`, `proto/ledger/v1/ledger.proto`,
`internal/api/server.go`, `internal/api/rest.go`

This is the single biggest throughput lever in the project, and the
implementation teaches Go concurrency at its most practical: how to turn N
independent request/response round-trips into ONE submission with one
completion — without races, without hangs, and without abandoning
cancellation semantics.

---

## 1. Why batching changes everything

Measured on the same machine, through the full gRPC stack
(`tests/integration/grpc_bench_test.go`):

| Path | ns/op | What an "op" is | Throughput |
|---|---|---|---|
| Unary `CreateTransfer` | 4,729,317 | 1 transfer | **~211 TPS** |
| Batch `CreateTransfers` | 30,481,781 | 8,192 transfers | **~268,700 TPS** |

**Ratio: ~1,270×** (target was ≥25×). Two forces produce this:

1. **fsync amortization.** A unary transfer waits for its own engine batch:
   collect (1 ms timeout) + fsync. A batch of 8192 pays ONE fsync for 8192
   transfers. Cost per transfer: `(collect + fsync) / 8192`.
2. **RPC overhead amortization.** The unary pprof profile is dominated by
   `pthread_cond_wait`/`pthread_cond_signal` — goroutine park/unwake per RPC,
   protobuf encode/decode per transfer, connection bookkeeping. The batch
   profile's CPU is dominated by `syscall.rawsyscalln` (the fsync) — the
   engine itself nearly vanishes:

   ```
   batch CPU profile (top):          batch alloc profile (top):
   syscall.rawsyscalln    24.5%      protobuf decode (proto internals) ~50%
   pthread_cond_signal    22.8%      test-side transfer construction    15%
   pthread_cond_wait      18.5%      wal.EncodeRecord                    4%
   runtime.usleep         15.3%      core.EncodeTransferPayload          4%
   ...engine orchestration ~0.02%    engine.Ledger.CreateTransfers   0.017%
   ```

The lesson generalizes: **at high throughput, per-unit costs are the enemy,
and the per-unit cost of "doing a network round-trip" dwarfs the per-unit
cost of the actual work.** TigerBeetle's ~8k-transfers-per-message limit is
exactly this arithmetic.

## 2. The engine protocol: one submit, one result slice, one notification

```go
// internal/engine/ledger.go
func (l *Ledger) CreateTransfers(ctx context.Context, batch []core.Transfer) ([]TransferOutcome, error)
```

Four phases:

- **Phase 1 — reserve idempotency.** Per item: zero keys skip dedup; keys
  already committed return the original transfer as that item's outcome;
  keys reserved now are tracked for commit/rollback later. Nothing is
  submitted, so an abort here rolls every reservation back cleanly.
- **Phase 2 — submit.** Each item becomes a `TransferEvent` stamped with the
  batch's shared `batchState` and its index; events go into the ordinary
  ring buffer. **No new queue was built** — the batch rides the existing
  single-writer drain, so events from many client batches interleave into
  one engine batch, one `AppendBatch`, one fsync.
- **Phase 3 — wait once.** `<-bs.done`. That is the entire completion
  protocol: the event loop's `complete(ack)` decrements an atomic counter
  and closes the channel when it hits zero.
- **Phase 4 — materialize.** Positional outcomes are filled from the batch's
  result slice; successful keys commit to idempotency, failed keys roll
  back.

The deep design decision: **batch state lives above the chunking.** A client
batch may be applied across several engine batches ( interleaved with other
clients' events, split by ring-buffer drains). `batchState.remaining` counts
*events*, not engine batches, so completion fires exactly when the last event
lands — the same principle as C0.17's recovery fix ("state machines must
outlive the boundaries of their input chunks"), on the write side this time.

## 3. The completion edge: atomic countdown + once-guarded close

```go
// internal/engine/event.go
type batchState struct {
    results   []TransferAck
    remaining atomic.Int64
    doneOnce  sync.Once
    done      chan struct{}
}

func (b *batchState) complete(index int, ack TransferAck) {
    b.results[index] = ack
    if b.remaining.Add(-1) == 0 {
        b.doneOnce.Do(func() { close(b.done) })
    }
}
```

Why each piece exists:

- **Distinct-index writes.** Each result slot has exactly one writer: the
  loop for submitted events, the submitter for events it *knows* were never
  queued (shutdown path). Distinct elements of one slice from two goroutines
  is race-free — no lock needed.
- **`atomic.Add(-1) == 0`.** The countdown is read-modify-write from both
  goroutines; only an atomic makes the total order well-defined. The goroutine
  that observes zero *knows* it completed the last event.
- **`sync.Once` for the close.** Double-close of a channel panics; `Once`
  makes the "fire exactly one completion notification" invariant structural
  rather than conventional.
- **Happens-before.** The submitter reads `results` only after `<-done`
  returns; a channel receive that observes a close synchronizes-after the
  close, which synchronizes-after every write that preceded it. No mutex
  anywhere in the result path.

Contrast with the naive design — one `chan error` per transfer (the unary
path's original shape): N channel allocations, N loop-side sends, N caller
receives, and the caller must count N replies itself. The batchState fuses
all of that into one atomic decrement per event.

## 4. The ack now carries the committed transfer

While wiring the batch API, a latent quirk surfaced: the event loop assigns
server-side timestamps to `events[i].Transfer` (its own copy), but unary
acks carried only the `error` — so callers always saw the timestamp-less
original. The fix benefits both paths:

```go
type TransferAck struct {
    Transfer core.Transfer // includes the server-assigned timestamp
    Err      error
}
```

The loop completes each event with its (possibly timestamp-updated) copy.
Batch outcomes and unary returns now both observe *what actually landed* —
and the idempotency store commits the timestamped version, consistent with
what recovery will later decode from the WAL. Lesson: **the ack should
describe the committed state, not just the attempt.**

## 5. Cancellation semantics, honestly

The hard corner of any batch protocol is cancellation mid-flight. This
implementation draws the line TigerBeetle draws:

- **Before submission** (Phase 1): the request context cancels cleanly; all
  reservations roll back; nothing was applied.
- **After submission**: a queued transfer runs to completion — cancelling
  the request does not un-apply it. The submitter still waits for
  `bs.done`, which is *bounded* (one engine batch cycle), so the RPC
  response is only marginally delayed; the idempotency keys of successes
  are committed, so a client retry is deduplicated instead of
  double-spending. This closes the abandonment window the unary path
  documents but tolerates.
- **Ledger shutdown mid-submission**: submit retries honor `l.ctx` (the
  ledger's lifecycle), not the request. On shutdown, the never-queued tail
  is completed *by the submitter* (`ErrLedgerClosed`) — safe because the
  loop provably never saw those events — the submitted prefix is completed
  by the loop's `abandonPending` drain, and the batch closes out with real
  results plus `context.Canceled`.

Trace why the submitter-completes-the-tail move is safe: `remaining` was
initialized to the full event count, every submitted event will get exactly
one loop-side completion (processed normally, or `ErrLedgerClosed` from
`abandonPending`), and every never-submitted event gets exactly one
submitter-side completion. The countdown always reaches zero; the wait
always terminates; `doneOnce` makes the single-close guarantee hold even
though two goroutines participate.

## 6. Within-batch duplicate keys: the deadlock that tests caught

The first implementation spun on `ErrIdempotencyConflict` the way the unary
path does. Within one batch that **deadlocks**: item 0 reserves key K and
its commit lands only after the whole batch completes — so item 1's
conflict-retry spin never terminates (the integration test hung, and only
the `-timeout` flag revealed it). The fix distinguishes the two conflict
kinds:

- **Same batch**: a `seenKeys` map fails item *i* immediately with
  `ErrIdempotencyConflict` — it never reserves, never spins.
- **Cross-caller**: the spin remains correct (the other caller's commit is
  one batch cycle away).

General lesson: **a wait-for-X loop deadlocks when X is produced by the
waits itself.** Any time you retry on a condition that a later phase of your
own operation would resolve, you have built a cycle.

## 7. Accounts, REST, and the API surface

- **`CreateAccounts`** batches account creation: one WAL append + one fsync
  for the whole batch (previously N fsyncs!), per-item duplicate detection,
  positional `[]AccountOutcome`. Unary `CreateAccount` is now a batch of
  one.
- **gRPC** (`proto/ledger/v1`): `CreateTransfers` /
  `CreateAccounts` RPCs returning `repeated TransferResult{ok, transfer,
  error}`. Per-item failures ride inside a successful RPC — the batch is
  accepted at the protocol level; only whole-call problems (follower,
  oversize, empty) are RPC errors. Handler mapping reuses
  `transferErrorStatus` so unary and batch agree (C0.11's rule).
- **REST** (`POST /v1/transfers/batch`, `/v1/accounts/batch`): arrays in,
  `{"results":[{"index":0,"ok":true,"transfer":{...}} | {"index":1,"ok":
  false,"error":"insufficient funds..."}]}` out, HTTP 200 with per-item
  status.

> **Exercise 1.** Run the proof: `go test -run '^$' -bench
> 'BenchmarkGRPC(Unary|Batch)' -benchtime 200x ./tests/integration` and
> compute the ratio `(8192 × unary_ns) / batch_ns`. Then run batch sizes
> 1/128/1024/8192 (`BenchmarkLedgerCreateTransfersBatch`) and plot TPS vs
> size — where does the curve flatten, and why?
>
> **Exercise 2.** The batch CPU profile shows `syscall.rawsyscalln` (fsync)
> at ~25% and proto decode at ~50% of allocations. Given S2.1 (single-buffer
> batch encode, one sequential `Write`), predict which allocations disappear
> and re-run `go tool pprof -sample_index=alloc_objects` to check.
>
> **Exercise 3.** Kill the server mid-batch (cancel `l.ctx` from a test,
> with a submitter in flight) and verify: queued transfers apply, never-
> queued ones report `ErrLedgerClosed`, no goroutine hangs. This is the
> shutdown-consistency test every batch system needs.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Amortize the constant | one fsync, one RPC per batch | Per-unit overhead dominates at scale |
| State above chunking | `batchState.remaining` | Batch state counts events, not engine batches |
| Countdown + Once close | `complete()` | Atomic decrement, `sync.Once` close, happens-before via close |
| Ack carries committed state | `TransferAck` | Return what landed (timestamps), not what was attempted |
| Cancellation line | pre- vs post-submit | Queued work completes; keys commit so retries dedup |
| Wait-for-X cycles | duplicate keys in batch | A retry condition your own operation must produce deadlocks |
| Protocol-level acceptance | per-item results in 200/OK | Batch accepted ⇒ per-item status, not whole-call errors |
