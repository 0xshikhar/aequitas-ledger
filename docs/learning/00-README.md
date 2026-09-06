# Learning Go Deeply Through aequitas-ledger

This series teaches Go — deeply — by walking through every subsystem of this
repository and asking, for each line of significant code: *why this construct,
why not the alternative, and what breaks if you get it wrong?* Every document
references real files in this repo, and every hard-won lesson comes from real
bugs this project actually had (catalogued as C0.x in `docs/ROADMAP.md`).

## Who this is for

You know Go syntax: functions, structs, slices, goroutines, channels. You want
the next level — the mechanics underneath: memory layout, the memory model,
what fsync actually promises, why a benchmark lied to us, and how a per-segment
state bug silently deleted committed financial transactions.

## The reading order

Read in order for a full course, or jump to the topic you need. Each document
stands alone, but the bug stories build on each other.

| # | Document | Go concepts | System |
|---|---|---|---|
| 01 | [Value semantics & memory layout](01-value-semantics-and-memory.md) | structs, arrays vs slices, zero values, alignment, compile-time guards | `internal/core` |
| 02 | [The single-writer concurrency model](02-single-writer-concurrency.md) | goroutines, channels, select, context, WaitGroup, shutdown ordering | `internal/engine` |
| 03 | [Atomics & the lock-free ring buffer](03-atomics-and-lock-free-structures.md) | CAS loops, memory model, happens-before, false sharing | `internal/engine/ring_buffer.go` |
| 04 | [File I/O, fsync & durability](04-file-io-and-durability.md) | os.File, WriteAt, page cache, fsync, atomic rename, recovery | `internal/wal`, `internal/snapshot` |
| 05 | [Binary formats, framing & CRC](05-binary-formats-and-crc.md) | encoding/binary, length-prefix framing, checksums, versioning | `internal/wal/record.go`, `internal/core/codec.go` |
| 06 | [Database-grade error handling](06-error-handling.md) | typed errors, errors.Is/As, panic policy, API error mapping | `internal/core/errors.go`, `internal/api` |
| 07 | [Testing like a database](07-testing-like-a-database.md) | table tests, race detector, fuzzing, property tests, allocation bounds | `tests/` |
| 08 | [Benchmarking & performance engineering](08-benchmarking-and-performance.md) | ReportAllocs, escape analysis, profiling, allocation hunting | `bench/`, hot paths |
| 09 | [APIs: gRPC, REST & middleware](09-apis-grpc-rest.md) | protobuf, interceptors, net/http, context values | `internal/api`, `proto/` |
| 10 | [Observability, config & lifecycle](10-observability-and-operations.md) | Prometheus, slog, pprof, env config, signals, graceful drain | `internal/observability`, `cmd/server` |
| 11 | [Modules, layout & dependency hygiene](11-modules-and-project-layout.md) | go.mod, internal/, cmd/ pattern, go mod tidy, import cycles | repo root |
| 12 | [The batched request/response protocol](12-batched-api.md) | shared completion states, atomic countdowns, cancellation semantics, amortization math | `engine.CreateTransfers`, batch RPCs |

## The running case studies

Three real bugs from this project's history recur across the series. Watch for
them — they are the most instructive material here:

1. **The 1.2 MB batch** (C0.15). A `make([]TransferEvent, 0, maxBatchSize)`
   preallocation turned a batch of one transfer into a 1.2 MB allocation.
   Teaches: slice capacity, allocation profiling, and "measure, don't guess."
2. **The starving reader** (C0.16). A `select` with a `default:` branch plus an
   unconditional batcher spin starved all reads under write load — p50 read
   latency went from 1 ms to 21.9 µs after the fix. Teaches: select semantics,
   fair scheduling, latency budgets.
3. **The cross-segment batch** (C0.17). Batch-atomicity state lived per
   segment, so a batch straddling a segment rotation was truncated as a "torn
   tail" and its successors deleted — committed transfers, gone. Teaches:
   state machine scope, recovery design, and why property tests exist.

## How to work through it

For each document:

1. Read the Go mechanics section.
2. Open the referenced files and find the constructs in situ.
3. Run the associated tests: `go test -race ./tests/...`
4. Do the exercises at the end — they are small, concrete, and mostly
   "break it and watch what happens."

## Running the project

```sh
go build ./...                      # compile everything
go test -race ./...                 # full suite with race detector
go test -race -run TestReadLatency ./tests/integration -v
go test -bench=BenchmarkWALTruncateBefore -benchmem ./tests/unit
go run ./cmd/server                  # primary on :50051/:8080/:6060
LEDGER_ROLE=follower go run ./cmd/server  # read-only follower
```

Recommended tooling: `go vet ./...`, `gofmt -l .`, and (once CI lands)
`golangci-lint`. The `-race` flag is not optional in this codebase — document
02 explains why.
