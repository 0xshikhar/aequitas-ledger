# aequitas-ledger — Implementation Status Plan

This file is the **single source of truth** for step-by-step implementation progress.

## Legend

- `⬜ Not started`
- `🟨 In progress`
- `✅ Done`
- `⛔ Blocked`

---

## Project Goal

Build a high-throughput, crash-safe, single-writer double-entry ledger in Go with:

- strict invariant: `SUM(debits) == SUM(credits)`
- WAL durability + deterministic replay
- benchmark evidence vs naive Postgres baseline

---

## Global Definition of Done

- [x] All core + unit tests pass
- [x] Integration tests (including crash recovery) pass
- [x] Benchmark report generated and committed (`docs/benchmark_report.md`)
- [x] ADRs complete and linked (`docs/adr/`)
- [x] README architecture claims are demonstrably true in code/tests

---

## Phase-by-phase execution plan

## Phase 0 — Foundations
**Status:** ✅ Done  
**Outcome:** domain primitives + binary record format are stable and tested.

### Tasks
- [x] Implement `internal/core/uint128.go`
  - [x] `Add`, `Sub`, `Cmp`, `IsZero`, `FromUint64`, `FromString`, `String`
  - [x] `MarshalBinary`, `UnmarshalBinary`
- [x] Implement `internal/core/account.go`
  - [x] `Account` value type with fixed fields and padding
  - [x] `Balance`, `IsFrozen`, `IsClosed`
- [x] Implement `internal/core/transfer.go`
- [x] Implement typed errors in `internal/core/errors.go`
- [x] Implement WAL record codec in `internal/wal/record.go`
  - [x] format `[type|len|payload|crc32]`
  - [x] corruption detection + graceful decode failure

### Validation gate
- [x] `go test ./internal/core/... ./internal/wal/...`
- [x] Unit tests for `tests/unit/uint128_test.go`
- [x] Decoder fuzz/property tests (no panic on malformed bytes)

---

## Phase 1 — WAL durability layer
**Status:** ✅ Done  
**Outcome:** append-only segments, group commit primitives, robust recovery.

### Tasks
- [x] Implement `internal/wal/segment.go`
  - [x] segment open/rotate/full checks
  - [x] typed write/read APIs
- [x] Implement `internal/wal/wal.go`
  - [x] `Open`, `AppendBatch`, `Sync`, `CurrentLSN`, `Close`
  - [x] one `Sync()` per batch model
- [x] Implement `internal/wal/recovery.go`
  - [x] replay in LSN order
  - [x] truncated tail detection
  - [x] CRC failure handling

### Validation gate
- [x] `go test ./internal/wal/...`
- [x] `tests/unit/wal_test.go`
  - [x] append + recover happy path
  - [x] truncated/corrupt tail handling

---

## Phase 2 — Engine state machine (no API yet)
**Status:** ✅ Done  
**Outcome:** deterministic, lock-free, single-writer transfer execution.

### Tasks
- [x] Implement `internal/engine/accounts.go`
  - [x] account create/get
  - [x] transfer validation rules
  - [x] debit/credit mutations
  - [x] snapshot copy for readers/checkpointer
- [x] Implement `internal/engine/transfers.go`
  - [x] `ValidateBatch`
  - [x] `ApplyBatch` in strict slice order
- [x] Implement eventing queue components
  - [x] `internal/engine/event.go`
  - [x] `internal/engine/ring_buffer.go`
  - [x] `internal/engine/batch.go`
- [x] Implement main event loop in `internal/engine/loop.go`
  - [x] drain → validate → WAL append/sync → apply → ack
- [x] Implement idempotency store in `internal/engine/idempotency.go`
  - [x] TTL + LRU bounded memory behavior
- [x] Wire façade in `internal/engine/ledger.go`

### Validation gate
- [x] `go test ./internal/engine/...`
- [x] Unit tests for batch ordering + queue correctness
- [x] Integration test: concurrent submitters, invariant preserved
- [x] Local throughput smoke test (`go test -bench=. ./bench/...` ~0.34ms/op, ~2,900 TPS)

---

## Phase 3 — Recovery integration at startup
**Status:** ✅ Done  
**Outcome:** crash + restart produces correct balances and preserved invariant.

### Tasks
- [x] Wire recover-on-start in `cmd/server/main.go`
- [x] Replay WAL records into engine state before serving traffic
- [x] Add/finish `tests/integration/crash_recovery_test.go`
  - [x] simulate unclean stop mid-stream
  - [x] restart + replay
  - [x] compare recovered balances to expected truth

### Validation gate
- [x] `go test ./tests/integration -run CrashRecovery`
- [x] invariant check passes after replay

---

## Phase 4 — gRPC API surface
**Status:** ✅ Done  
**Outcome:** external API supports account + transfer operations with middleware guarantees.

### Tasks
- [x] Define/verify `proto/ledger/v1/ledger.proto`
  - [x] account/transfer/money messages
  - [x] service RPC methods
- [x] Generate protobuf stubs (`ledger.pb.go`, `ledger_grpc.pb.go`)
- [x] Implement API server wiring in `cmd/server/main.go` & `internal/api/server.go`
- [x] Implement handlers (`CreateAccount`, `GetAccount`, `CreateTransfer`)
- [x] Implement middleware (`internal/api/idempotency.go`)

### Validation gate
- [x] `go test ./tests/integration -run Transfers`
- [x] gRPC round-trip tests for create/get/list paths
- [x] concurrent API submit test keeps invariant true

---

## Phase 5 — Observability + operations visibility
**Status:** ✅ Done  
**Outcome:** measurable performance and production-style debugging visibility.

### Tasks
- [x] Implement metrics in `internal/observability/metrics.go`
  - [x] transfer count
  - [x] batch size histogram
  - [x] WAL sync duration
  - [x] queue depth
- [x] Implement `internal/observability/logger.go`
- [x] Implement `internal/observability/pprof.go`
- [x] Ensure `docker-compose.yml` includes Prometheus/Grafana wiring

### Validation gate
- [x] Metrics endpoint exports expected series (`/metrics`)
- [x] pprof endpoint reachable (`/debug/pprof/`)
- [x] Prometheus/Grafana service definitions in `docker-compose.yml`

---

## Phase 6 — Benchmark artifact (portfolio signal)
**Status:** ✅ Done  
**Outcome:** reproducible benchmark comparison with analysis.

### Tasks
- [x] Implement Postgres baseline store
  - [x] `internal/store/postgres/store.go`
  - [x] migration `001_init.sql`
- [x] Implement bench harness in `bench/`
  - [x] `harness.go`
  - [x] `bench_test.go`
  - [x] `cmd/bench/main.go`
- [x] Capture benchmark comparison harness executable and automated test suite

### Validation gate
- [x] `go test -bench=. -benchmem ./bench/...`
- [x] CLI runner `cmd/bench/main.go` generated
- [x] comparison suite supporting TPS + latency metrics

---

## Phase 7 — Copy-on-Write (CoW) In-Memory Snapshotter
**Status:** ✅ Done  
**Outcome:** Fast startup recovery from point-in-time snapshots and delta WAL replay.

### Tasks
- [x] Implement `internal/snapshot/snapshot.go` and `internal/snapshot/manager.go`
- [x] Wire snapshot recovery into engine initialization
- [x] Author Phase 7 educational guide (`docs/phases/phase-7-snapshots.md`)

### Validation gate
- [x] `go test -v ./internal/snapshot/...`

---

## Phase 8 — REST Gateway & Transactional Event Outbox
**Status:** ✅ Done  
**Outcome:** REST HTTP interface and transactional outbox event publisher.

### Tasks
- [x] Implement REST server (`internal/api/rest.go`)
- [x] Implement transactional outbox publisher (`internal/events/publisher.go`)
- [x] Add REST integration test suite (`tests/integration/rest_test.go`)
- [x] Author Phase 8 educational guide (`docs/phases/phase-8-rest-and-outbox.md`)

### Validation gate
- [x] `go test -v ./tests/integration -run TestRESTAPI`

---

## Phase 9 — Primary-Follower WAL Streaming Replication
**Status:** ✅ Done  
**Outcome:** Real-time gRPC stream replication to follower replica nodes.

### Tasks
- [x] Implement replication gRPC server (`internal/replication/server.go`)
- [x] Implement follower catchup worker (`internal/replication/follower.go`)
- [x] Add replication integration test (`tests/integration/replication_test.go`)
- [x] Author Phase 9 educational guide (`docs/phases/phase-9-replication.md`)

### Validation gate
- [x] `go test -v ./tests/integration -run TestReplication`

---

## Phase 10 — Production Hardening, Config System & Replica Promotion
**Status:** ✅ Done  
**Outcome:** Centralized ENV config manager, follower promotion failover logic, failover integration test, and Phase 10 educational guide.

### Tasks
- [x] Implement centralized config manager (`internal/config/config.go`)
- [x] Implement Follower promotion to Primary (`internal/replication/failover.go`)
- [x] Wire `config.Load()` into `cmd/server/main.go`
- [x] Add failover integration test (`tests/integration/failover_test.go`)
- [x] Authored Phase 10 educational guide (`docs/phases/phase-10-production-hardening-and-failover.md`)

### Validation gate
- [x] `go test -v ./tests/integration -run TestFailover`
- [x] `go test -v ./...`

---

## Phase 11 — Interactive Operator CLI Tool
**Status:** ✅ Done  
**Outcome:** A robust command-line interface tool (`ledger-cli`) to interact with the Ledger's REST API seamlessly.

### Tasks
- [x] Create `cmd/ledger-cli/main.go` with `account` and `transfer` subcommands
- [x] Implement end-to-end CLI integration testing (`tests/integration/cli_test.go`)
- [x] Author Phase 11 educational guide (`docs/phases/phase-11-operator-cli.md`)

### Validation gate
- [x] `go test -v ./tests/integration -run TestCLITool`

---

## Phase 12 — Docker Compose & Kubernetes Production Deployment Manifests
**Status:** ✅ Done  
**Outcome:** Multi-stage non-root Dockerfiles, HA Primary-Follower Docker Compose stack, Kubernetes StatefulSet & Deployment manifests, and Phase 12 educational guide.

### Tasks
- [x] Multi-stage non-root `Dockerfile` & `Dockerfile.cli`
- [x] Primary-Follower topology in `docker-compose.yml`
- [x] Kubernetes manifests (`configmap.yaml`, `statefulset-primary.yaml`, `deployment-follower.yaml`, `service.yaml`)
- [x] Author Phase 12 educational guide (`docs/phases/phase-12-containerization-and-k8s.md`)

### Validation gate
- [x] `go test -v ./...`

---

## Phase 13 — Architecture Decision Records (ADRs) & Benchmarking Analysis Report
**Status:** ✅ Done  
**Outcome:** Complete suite of four formal ADRs in `docs/adr/`, empirical benchmark report (`docs/benchmark_report.md`), and Phase 13 educational guide.

### Tasks
- [x] Create ADR-001 (`docs/adr/001-single-writer-event-loop.md`)
- [x] Create ADR-002 (`docs/adr/002-wal-segment-and-group-commit.md`)
- [x] Create ADR-003 (`docs/adr/003-cow-snapshotting.md`)
- [x] Create ADR-004 (`docs/adr/004-primary-follower-replication.md`)
- [x] Generate empirical benchmark report (`docs/benchmark_report.md`)
- [x] Author Phase 13 educational guide (`docs/phases/phase-13-adrs-and-benchmarking.md`)
- [x] Update `docs/Status.md` with complete Phase 0–13 roadmap & Global Definition of Done validation

### Validation gate
- [x] `go test -bench=. -benchmem ./bench/...`
- [x] `go test -v ./...`

---

## Phase 14 — API Rate Limiting & Interceptor Security
**Status:** ⬜ Not started  
**Outcome:** Token bucket rate limiting middleware per API key and TLS/mTLS encryption for gRPC/REST endpoints.

### Tasks
- [ ] Implement token bucket rate limiter middleware (`internal/api/ratelimit.go`)
- [ ] Add TLS / mTLS configuration flags to ENV config (`internal/config/config.go`)
- [ ] Add rate limiting unit & integration tests

---

## Phase 15 — Advanced Balance Limits & Multi-Asset Validation Guards
**Status:** ⬜ Not started  
**Outcome:** Account overdraft limit enforcement (`MaxOverdraft`) and strict status guards (`IsFrozen`, `IsClosed`) in event loop validation.

### Tasks
- [ ] Implement `MaxOverdraft` credit limits in `internal/engine/accounts.go`
- [ ] Enforce account status check (`IsFrozen`, `IsClosed`) during batch validation
- [ ] Add unit tests for overdraft rejection and frozen account transfers

---

## Phase 16 — Dynamic Raft Leader Election & Multi-Node Consensus
**Status:** ⬜ Not started  
**Outcome:** HashiCorp Raft integration for dynamic leader election replacing manual failover promotion.

### Tasks
- [ ] Integrate `github.com/hashicorp/raft` in `internal/replication/consensus.go`
- [ ] Implement automated primary leader election and quorum WAL replication
- [ ] Add multi-node cluster failover integration test

---

## Phase 17 — Production High-Volume Load Generator (`cmd/loadgen`)
**Status:** ⬜ Not started  
**Outcome:** Dedicated CLI load generator capable of simulating 10,000+ parallel client connections.

### Tasks
- [ ] Build `cmd/loadgen/main.go` with configurable concurrency workers & TPS targets
- [ ] Measure P50, P95, P99 gRPC latency distributions under 1-hour sustained stress runs
- [ ] Generate load test report

---

## Phase 18 — OpenTelemetry Distributed Tracing & Production Alerting
**Status:** ⬜ Not started  
**Outcome:** End-to-end OTel tracing spans and production Grafana alert rules.

### Tasks
- [ ] Instrument gRPC server, REST server, and WAL sync calls with OpenTelemetry SDK
- [ ] Create Grafana Prometheus alert rules (`deploy/prometheus_alerts.yml`) for WAL sync latency & invariant failures

---

## Phase 19 — Chaos Engineering Suite & Operational Runbooks
**Status:** ⬜ Not started  
**Outcome:** Automated chaos test harness and operational disaster recovery runbook.

### Tasks
- [ ] Build chaos testing script (`tests/chaos/chaos_test.go`) simulating disk space exhaustion & network packet drops
- [ ] Author production operational disaster recovery runbook (`docs/runbook.md`)

---

## Execution order (strict)

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4
6. Phase 5
7. Phase 6
8. Phase 7
9. Phase 8
10. Phase 9
11. Phase 10
12. Phase 11
13. Phase 12
14. Phase 13
15. Phase 14
16. Phase 15
17. Phase 16
18. Phase 17
19. Phase 18
20. Phase 19

Do not skip ahead before each phase’s validation gate passes.

---

## Progress log

| Date | Phase | Update | Status |
|---|---|---|---|
| 2026-08-10 | Planning | Created initial step-by-step roadmap from `docs/README.md` architecture and implementation intent. | ✅ Done |
| 2026-08-10 | Phase 0 | Implemented core foundation types (`Uint128`, `Account`, `Transfer`, typed errors), WAL record codec with CRC32, plus unit + fuzz tests. | ✅ Done |
| 2026-08-10 | Phase 1 | Implemented WAL segments, batch append/sync API, and crash-tail recovery with truncation; added WAL append/recover tests. | ✅ Done |
| 2026-08-10 | Phase 2 | Implemented engine state machine (accounts, transfers, ring buffer, batcher, loop, idempotency, ledger) with unit/integration coverage. | ✅ Done |
| 2026-08-10 | Phase 3 | Implemented server startup recovery entrypoint `cmd/server/main.go` and comprehensive integration test `tests/integration/crash_recovery_test.go` verifying balance equality and double-entry invariant post ungraceful restart. | ✅ Done |
| 2026-08-10 | Phase 4 | Defined `proto/ledger/v1/ledger.proto`, compiled protobuf stubs, built gRPC handlers, idempotency header interceptor middleware, and comprehensive gRPC integration tests. | ✅ Done |
| 2026-09-01 | Phase 5 | Built observability package (`internal/observability`) with Prometheus metrics, structured `slog` logger, `/metrics` and `/debug/pprof/` HTTP endpoints, and `docker-compose.yml` for Prometheus/Grafana stack. | ✅ Done |
| 2026-09-01 | Phase 6 | Built Postgres baseline store (`internal/store/postgres`), load harness (`bench/harness.go`, `bench/bench_test.go`), and benchmark CLI (`cmd/bench/main.go`). | ✅ Done |
| 2026-09-01 | Phase 7 | Implemented async CoW snapshot checkpointer (`internal/snapshot/`), fast recovery replay from LSN checkpoints, and authored complete Phase 0–7 Go language & architecture educational guide suite (`docs/phases/`). | ✅ Done |
| 2026-09-01 | Phase 8 | Implemented REST HTTP Gateway (`internal/api/rest.go`), transactional outbox event publisher (`internal/events/publisher.go`), REST integration test suite, and Phase 8 educational guide. | ✅ Done |
| 2026-09-01 | Phase 9 | Implemented Primary WAL stream replication server (`internal/replication/server.go`), Follower catchup worker (`internal/replication/follower.go`), replication integration test, and Phase 9 educational guide. | ✅ Done |
| 2026-09-01 | Phase 10 | Implemented centralized ENV config manager (`internal/config/config.go`), replica promotion method (`internal/replication/failover.go`), failover integration test, and Phase 10 educational guide. | ✅ Done |
| 2026-09-01 | Phase 11 | Built Interactive Operator CLI Tool (`ledger-cli`), end-to-end integration tests, and Phase 11 educational guide. | ✅ Done |
| 2026-09-01 | Phase 12 | Built production Dockerfiles, HA Docker Compose stack, Kubernetes StatefulSets & Deployments, and Phase 12 educational guide. | ✅ Done |
| 2026-09-01 | Phase 13 | Completed formal ADR suite (`docs/adr/`), empirical performance benchmark analysis (`docs/benchmark_report.md`), Phase 13 guide, and verified Global Definition of Done. | ✅ Done |
| 2026-09-01 | Phase 14 | API Rate Limiting & Interceptor Security | ⬜ Not started |
| 2026-09-01 | Phase 15 | Advanced Balance Limits & Multi-Asset Validation Guards | ⬜ Not started |
| 2026-09-01 | Phase 16 | Dynamic Raft Leader Election & Multi-Node Consensus | ⬜ Not started |
| 2026-09-01 | Phase 17 | Production High-Volume Load Generator (`cmd/loadgen`) | ⬜ Not started |
| 2026-09-01 | Phase 18 | OpenTelemetry Distributed Tracing & Production Alerting | ⬜ Not started |
| 2026-09-01 | Phase 19 | Chaos Engineering Suite & Operational Runbooks | ⬜ Not started |

