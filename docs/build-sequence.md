# Build sequence

This document specifies the exact order to implement the ledger. The ordering ensures that each phase produces something testable before you add the next layer.

---

## Phase 0 — Foundations (build these before anything else)

These three things must exist before you write a single line of engine code. Starting without them forces a rewrite.

**0.1 `internal/core/uint128.go`**  
Build `Uint128`, all arithmetic functions, `MarshalBinary`, `UnmarshalBinary`, `FromString`, `String`. Write `tests/unit/uint128_test.go` with overflow, underflow, and round-trip cases. Run the fuzz test. Do not proceed until all tests pass.

**0.2 `internal/core/account.go` + `transfer.go` + `errors.go`**  
Define `Account` and `Transfer` as value types using `Uint128`. Check that `sizeof(Account) == 64` — add padding if not. Define all error types.

**0.3 `internal/wal/record.go`**  
Define the binary record format. Write encoder and decoder with CRC32 verification. Fuzz test the decoder with random byte slices — it must never panic, only return `ErrCorrupted`.

Checkpoint: `go test ./internal/core/... ./internal/wal/...` passes.

---

## Phase 1 — The WAL (durability before state)

**1.1 `internal/wal/segment.go`**  
Fixed-size segment files. `openSegment`, `Write`, `ReadAt`, `IsFull`, `Rotate`.

**1.2 `internal/wal/wal.go`**  
`Open`, `AppendBatch`, `Sync`, `CurrentLSN`, `Close`. At this stage, `AppendBatch` writes records sequentially and `Sync` calls `fdatasync`. No recovery yet.

**1.3 `internal/wal/recovery.go`**  
`Recover`, `detectTruncation`, `replayRecord`. Write `tests/unit/wal_test.go`: append 1000 records, kill mid-write (truncate the file with `os.Truncate`), recover, verify last N complete records replayed correctly and no panic on the truncated tail.

Checkpoint: WAL append + crash recovery works. Every other layer depends on this.

---

## Phase 2 — The state machine (in isolation, no gRPC)

**2.1 `internal/engine/accounts.go`**  
`AccountManager` with `Create`, `Get`, `ValidateTransfer`, `ApplyDebit`, `ApplyCredit`, `Snapshot`. Write unit tests verifying balance invariant: for any sequence of debits and credits, `PostedCredits - PostedDebits == Balance`.

**2.2 `internal/engine/transfers.go`**  
`ValidateBatch`, `ApplyBatch`. Unit test: submit a batch with valid and invalid transfers interleaved, verify only valid ones mutate balances, verify ordering preserved.

**2.3 `internal/engine/event.go` + `ring_buffer.go` + `batch.go`**  
`TransferEvent`, `RingBuffer`, `Batcher`. Unit test: 100 goroutines submit concurrently, single consumer drains, verify no events lost, no events duplicated.

**2.4 `internal/engine/loop.go`**  
`EventLoop.Run`, `processBatch`. Integration test: spin up the loop, submit 10,000 transfers from 50 goroutines, verify all complete, verify double-entry invariant holds across all accounts.

**2.5 `internal/engine/idempotency.go`**  
`IdempotencyStore`. Unit test: submit the same key twice concurrently, verify only one transfer executes, both callers get the same result.

**2.6 `internal/engine/ledger.go`**  
Wire everything together. `NewLedger`, `CreateTransfer`, `GetAccount`. Integration test: the full path from `CreateTransfer` call through the ring buffer, loop, WAL, and back to the caller.

Checkpoint: `go test ./internal/engine/... ./internal/wal/...` passes. Run a manual throughput check: `CreateTransfer` in a tight loop, measure TPS. Target > 50k TPS on a single goroutine before touching gRPC.

---

## Phase 3 — WAL recovery integration

**3.1 `cmd/server/main.go` — recovery path only**  
Before starting the event loop: call `wal.Recover()` which calls `ledger.Recover()`. Replay all WAL records into `AccountManager`.

**3.2 `tests/integration/crash_recovery_test.go`**  
- Create accounts, run 10 batches of transfers, record balances.
- Simulate crash: stop the loop, do NOT close the WAL cleanly (to simulate a mid-batch crash).
- Restart: recover from WAL.
- Assert recovered balances == ground truth.
- Assert double-entry invariant holds.

This test is the most important test in the project. If it fails, the ledger is not production-safe.

---

## Phase 4 — gRPC API

**4.1 `proto/ledger/v1/ledger.proto`**  
Define `Account`, `Transfer`, `Money` (using two uint64 fields), all RPC methods. Run `protoc` to generate Go bindings.

**4.2 `internal/api/` — handlers and middleware**  
`accounts_handler.go`, `transfers_handler.go`. Idempotency middleware reads the `Idempotency-Key` gRPC metadata header, hashes it to `[32]byte`, passes to `ledger.CreateTransfer`.

**4.3 `tests/integration/transfers_test.go`**  
Full gRPC round-trip: create accounts, create transfers via gRPC, verify balances via gRPC. Include concurrent test: 100 goroutines each submitting 1,000 transfers, verify invariant at the end.

---

## Phase 5 — Observability

**5.1 `internal/observability/metrics.go`**  
Register Prometheus counters/histograms. Instrument `processBatch` (batch size, WAL sync duration, apply duration). Instrument `RingBuffer` (current length, submit rate, drain rate).

**5.2 `internal/observability/pprof.go`**  
Start `/debug/pprof` HTTP server on a side port. This is what you use to prove the GC is not thrashing.

**5.3 `docker-compose.yml`**  
Add Prometheus scraping the ledger's metrics endpoint. Add Grafana with a pre-built dashboard JSON.

---

## Phase 6 — Benchmarks (the portfolio artifact)

**6.1 `internal/store/postgres/store.go`**  
Implement the naive Postgres baseline: `BEGIN`, `SELECT FOR UPDATE` on both accounts, two `UPDATE` statements, `COMMIT`. One gRPC call = one transaction = one fsync.

**6.2 `bench/` — harness, scenarios, report**  
Implement all scenarios: sequential, concurrent, hot-account, mixed read/write. Run both stores through all scenarios. Generate the comparison report.

**6.3 Profile and document**  
Run `go test -bench=. -benchmem` and `go tool pprof` on both stores. Screenshot the CPU profile showing that the ledger store's hot path is in `wal.Sync` (amortised across the batch) while the Postgres baseline's hot path is in `database/sql` lock wait. Include these profiles in the repo under `docs/profiles/`.

---

## Phase 7 — Operational (Bucket 2)

Only start these after Phase 6 benchmarks are green and documented.

**7.1 `internal/snapshot/`** — async checkpointer (ADR 006)

**7.2 Idempotency eviction** — TTL + LRU (ADR 005)

**7.3 `docs/adr/`** — ensure all six ADRs are complete and linked from README

---

## What "done" looks like for hiring purposes

A hiring panel reviewing this repo should be able to:

1. Read `README.md` and understand the architecture in 5 minutes
2. Read `docs/adr/001` through `006` and see the tradeoff reasoning
3. Run `make bench` and see the comparison output
4. Run `go test ./tests/integration/crash_recovery_test.go` and see it pass
5. Open a pprof CPU profile and see no GC pressure in the hot path
6. Ask "how would you add a second write partition?" and see the answer in the ADR notes

The TPS number matters less than the quality of the reasoning documented alongside it.
