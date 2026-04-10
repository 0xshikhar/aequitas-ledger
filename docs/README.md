# aequitas-ledger

A high-throughput, crash-safe, double-entry financial ledger built in Go — inspired by TigerBeetle's single-writer event loop architecture.

**Target:** 200k–500k TPS on commodity NVMe  
**Guarantee:** Strict double-entry invariant survives crashes and WAL replay  
**Signal:** Staff/principal-level systems design portfolio project

---

## What this project demonstrates

| Concept | Implementation |
|---|---|
| Single-writer event loop | `internal/engine/loop.go` — one goroutine owns all mutable state |
| WAL group commit | `internal/wal/wal.go` — `AppendBatch` + single `Sync()` per batch |
| Lock-free state machine | `internal/engine/accounts.go` — zero mutexes, pure value mutations |
| 128-bit money type | `internal/core/uint128.go` — handles crypto-native token precision |
| Deterministic replay | Batch processing over flat slices, never maps — order preserved |
| Idempotency w/ eviction | `internal/engine/idempotency.go` — TTL-based LRU, bounded memory |
| Backpressure | Ring buffer submit returns `ErrBufferFull` — API layer handles gracefully |
| Benchmark harness | `bench/` — compares ledger store vs naive Postgres baseline |

---

## Architecture overview

```
┌─────────────────────────────────────────────────────┐
│                   gRPC API layer                    │
│   accounts_handler · transfers_handler · middleware │
└────────────────────────┬────────────────────────────┘
                         │  submits TransferEvent{chan error}
                         ▼
┌─────────────────────────────────────────────────────┐
│              Ring buffer (submission queue)         │
│         lock-free enqueue · backpressure signal     │
└────────────────────────┬────────────────────────────┘
                         │  DrainBatch(max=10_000)
                         ▼
┌─────────────────────────────────────────────────────┐
│           Single-writer event loop (loop.go)        │
│                                                     │
│  1. Collect batch from ring buffer                  │
│  2. Validate all transfers (pure, no I/O)           │
│  3. WAL AppendBatch → single Sync()  [group commit] │
│  4. Apply mutations to AccountManager (no locks)    │
│  5. Ack each TransferEvent via chan error            │
└──────────┬──────────────────────┬───────────────────┘
           │                      │
           ▼                      ▼
┌─────────────────┐    ┌──────────────────────────────┐
│   WAL segments  │    │   In-memory AccountManager   │
│  append-only    │    │   value types, no GC pressure │
│  CRC-checked    │    │   read path via snapshot      │
└─────────────────┘    └──────────────────────────────┘
```

---

## Folder structure

```
aequitas-ledger/
├── cmd/
│   ├── server/
│   │   └── main.go                    # wire deps, start gRPC server, recover WAL
│   └── bench/
│       └── main.go                    # standalone benchmark runner
│
├── proto/
│   └── ledger/v1/
│       ├── ledger.proto               # Account, Transfer, service definitions
│       ├── ledger.pb.go               # generated — do not edit
│       └── ledger_grpc.pb.go          # generated — do not edit
│
├── internal/
│   ├── core/                          # shared domain types — no dependencies
│   │   ├── uint128.go                 # Uint128{Lo, Hi uint64} — the money type
│   │   ├── account.go                 # Account value type
│   │   ├── transfer.go                # Transfer value type
│   │   └── errors.go                  # typed ledger errors
│   │
│   ├── engine/                        # the state machine — owns all mutable state
│   │   ├── loop.go                    # THE single-writer goroutine
│   │   ├── ring_buffer.go             # lock-free submission queue
│   │   ├── batch.go                   # batch collector: drain + timeout
│   │   ├── event.go                   # TransferEvent{Transfer, Result chan error}
│   │   ├── ledger.go                  # Ledger — wires API to ring buffer
│   │   ├── accounts.go                # AccountManager — pure mutations, NO locks
│   │   ├── transfers.go               # batch transfer validation + application
│   │   └── idempotency.go             # IdempotencyStore — LRU with TTL eviction
│   │
│   ├── wal/
│   │   ├── wal.go                     # Open, AppendBatch, Sync, Recover, Close
│   │   ├── segment.go                 # fixed-size segment files
│   │   ├── record.go                  # encode/decode: [type|len|payload|crc32]
│   │   └── recovery.go                # replay segments in LSN order on startup
│   │
│   ├── store/
│   │   └── postgres/
│   │       ├── store.go               # PostgresStore — naive benchmark baseline
│   │       ├── queries.go             # typed SQL functions
│   │       └── migrations/
│   │           └── 001_init.sql       # accounts, transfers, idempotency_keys tables
│   │
│   ├── snapshot/
│   │   ├── checkpointer.go            # async snapshot writer — does NOT pause loop
│   │   └── snapshot.go                # serialise/deserialise AccountManager state
│   │
│   ├── api/
│   │   ├── server.go                  # LedgerServer struct + registration
│   │   ├── accounts_handler.go        # CreateAccount, GetAccount, GetBalance RPCs
│   │   ├── transfers_handler.go       # CreateTransfer, GetTransfer, ListTransfers RPCs
│   │   └── middleware/
│   │       ├── idempotency.go         # unary interceptor: key check before, commit after
│   │       ├── recovery.go            # panic → structured error
│   │       └── logging.go             # structured request/response logging
│   │
│   ├── config/
│   │   └── config.go                  # WAL dir, batch size, batch timeout, snapshot interval
│   │
│   └── observability/
│       ├── metrics.go                 # Prometheus: transfers_total, batch_size_hist, wal_sync_dur
│       ├── logger.go                  # slog wrapper with request-id injection
│       └── pprof.go                   # /debug/pprof on side port
│
├── bench/
│   ├── harness.go                     # BenchmarkHarness: goroutines, duration, stats
│   ├── scenarios.go                   # sequential, concurrent, hot-account, mixed
│   └── report.go                      # comparison table: ledger vs postgres
│
├── docs/
│   └── adr/
│       ├── 001-single-writer-event-loop.md
│       ├── 002-group-commit-wal.md
│       ├── 003-uint128-money-type.md
│       ├── 004-deterministic-batch-ordering.md
│       ├── 005-idempotency-eviction.md
│       └── 006-snapshot-checkpointer.md
│
├── tests/
│   ├── integration/
│   │   ├── transfers_test.go          # concurrent transfer correctness, invariant checks
│   │   ├── idempotency_test.go        # duplicate key → same result, no double-debit
│   │   └── crash_recovery_test.go     # kill mid-batch, restart, verify invariant holds
│   └── unit/
│       ├── uint128_test.go            # arithmetic, overflow detection
│       ├── wal_test.go                # append/recover, CRC corruption detection
│       └── batch_test.go              # ordering guarantees, timeout behaviour
│
├── docker-compose.yml                 # postgres + prometheus + grafana
├── Makefile                           # proto-gen, lint, test, bench, pprof targets
└── go.mod
```

---

## ADR index

| ADR | Decision | Status |
|---|---|---|
| [001](docs/adr/001-single-writer-event-loop.md) | Single-writer event loop over concurrent mutexes | Accepted |
| [002](docs/adr/002-group-commit-wal.md) | WAL group commit — one fsync per batch | Accepted |
| [003](docs/adr/003-uint128-money-type.md) | Uint128 as the canonical money type | Accepted |
| [004](docs/adr/004-deterministic-batch-ordering.md) | Flat slice ordering — no map iteration in state machine | Accepted |
| [005](docs/adr/005-idempotency-eviction.md) | LRU + TTL eviction for bounded idempotency memory | Accepted |
| [006](docs/adr/006-snapshot-checkpointer.md) | Async snapshot — no write-loop pause | Accepted |

---

## The core invariant

> **At all times:** `SUM(all debits) == SUM(all credits)` across the entire ledger.

Every test, every crash-recovery cycle, and every benchmark must verify this holds. The WAL replay, the deterministic batch ordering, and the single-writer model together make this provable — not just likely.
