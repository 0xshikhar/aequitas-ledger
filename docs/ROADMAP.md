# aequitas-ledger — Improvement Roadmap

**Date:** 2026-09-04
**Date:** 2026-09-04
**Purpose:** Close the gap between "feature-complete portfolio project" and "system a principal engineer would sign their name to," measured against the stated inspiration (TigerBeetle).

**How to read this:**
- Tiers are priority order. Tier 0 is *correctness debt* — it is the project's core claim ("strict invariant survives crashes and replay"). Until it's closed, nothing else compounds.
- Each item lists **Problem / Evidence / Fix / Proof**.
- **Status legend** per item: ⬜ Not started · 🟨 In progress · ✅ Done · ⛔ Blocked. Status is verified by the file/line/test referenced in **Evidence**, not by intent.
- **Profiling phase (current as of 2026-09-04):** `go build ./...` clean · `go test ./...` passes (≈15s) · `go test -race ./...` passes (≈13s). Six Tier-0 items (C0.1, C0.2, C0.3, C0.4, C0.6, C0.9) are now closed by recent commits. Four remain open (C0.5, C0.7, C0.8, C0.10) plus several newly-surfaced bugs added under "Newly surfaced" subsections in Tier 0.
**How to read this:**
- Tiers are priority order. Tier 0 is *correctness debt* — it is the project's core claim ("strict invariant survives crashes and replay"). Until it's closed, nothing else compounds.
- Each item lists **Problem / Evidence / Fix / Proof**.
- **Status legend** per item: ⬜ Not started · 🟨 In progress · ✅ Done · ⛔ Blocked. Status is verified by the file/line/test referenced in **Evidence**, not by intent.
- **Profiling phase (current as of 2026-09-04):** `go build ./...` clean · `go test ./...` passes (≈15s) · `go test -race ./...` passes (≈13s). Six Tier-0 items (C0.1, C0.2, C0.3, C0.4, C0.6, C0.9) are now closed by recent commits. Four remain open (C0.5, C0.7, C0.8, C0.10) plus several newly-surfaced bugs added under "Newly surfaced" subsections in Tier 0.

---

## 0. Honest current-state audit

### What is genuinely strong
- The architectural skeleton is right: MPMC ring buffer → batcher → single-writer loop → group-commit WAL → deterministic apply is the correct shape and mirrors TigerBeetle's design philosophy.
- `Uint128` + fixed-size 64-byte accounts with compile-time size guards; value semantics in the hot path; no `sync.Mutex` in the engine hot path.
- CRC-checked WAL records with `BatchCommit` framing; truncated-tail recovery now correctly stops at the last fully-committed batch.
- `Uint128` + fixed-size 64-byte accounts with compile-time size guards; value semantics in the hot path; no `sync.Mutex` in the engine hot path.
- CRC-checked WAL records with `BatchCommit` framing; truncated-tail recovery now correctly stops at the last fully-committed batch.
- Real (not toy) supporting surface: gRPC + REST, replication, failover, CLI, docker-compose/k8s, ADRs, benchmark harness with a Postgres baseline.
- Test discipline is above average: race tests, concurrent invariant tests, crash-recovery, snapshot+truncate, account durability, idempotency durability, failover, follower-mode rejection, CLI round-trip.
- Test discipline is above average: race tests, concurrent invariant tests, crash-recovery, snapshot+truncate, account durability, idempotency durability, failover, follower-mode rejection, CLI round-trip.

### What still undermines it today
1. **The core claim is partly proven, partly not.** Six Tier-0 items closed with passing tests under `-race`. Four remain: snapshot checkpointer not started in production, `core.Balance` still panics, replication apply has a per-record `AppendBatch` path that defeats group commit and a 10/s reconnect storm on bad CRC, and `C0.10` housekeeping is mostly unaddressed.
### What still undermines it today
1. **The core claim is partly proven, partly not.** Six Tier-0 items closed with passing tests under `-race`. Four remain: snapshot checkpointer not started in production, `core.Balance` still panics, replication apply has a per-record `AppendBatch` path that defeats group commit and a 10/s reconnect storm on bad CRC, and `C0.10` housekeeping is mostly unaddressed.
2. **Performance is 2,900 TPS, not 200k–500k.** `docs/benchmark_report.md` measures ~2,900 TPS (0.34 ms/op concurrent) yet concludes "verifying all performance design goals." The README target is 200k–500k TPS. A reviewer who reads both will trust nothing else in the repo.
3. **Newly-surfaced bugs the previous roadmap missed:**
   - `core.Balance` still `panic`s on underflow — only the *outer* `ApplyDebit` was hardened (C0.7 partial).
   - REST `/v1/transfers` and `/v1/accounts` don't map `core.ErrNotLeader` → 503, so a write against a follower returns generic 500.
   - REST `bytesToID` right-pads short hex IDs, silently aliasing `01` and `00000000000000000000000000000001`.
   - `NewLedger` calls `RecoverFromSnapshot(snapshotLSN)` after loading the snapshot into memory, which triggers a *full* WAL replay even on a snapshot-only path — wasted I/O and a foot-gun for future record types.
   - `peekSegmentMaxLSN` reads entire candidate segments into memory — every `TruncateBefore` call scans the whole dir.
   - `Batcher` + read/create queues are drained from the same `select` with `default:` → read latency is unbounded under heavy write load (no timer fairness).
   - `GetAccount` on the API allocates a fresh `chan ReadAccountResult` per request; combined with `TransferEvent` allocating `chan error` per submit, allocation pressure is the *first* thing to fix on the way to D1.1.
4. **Still unwired decoration:** `Checkpointer` exists but is never started in `cmd/server`; `events.Publisher` has no caller; `observability.RingBufferDepth` gauge is declared but never `.Set()`-ed; `deploy/prometheus.yml` scrapes `aequitas-ledger:6060` which matches no compose service; `deploy/k8s/deployment-follower.yaml` has no `volumes`/`volumeMounts`; `docs/` is untracked in git while `Status.md` claims "ADRs complete and committed."
5. **No CI, no lint, no `-race` enforcement, no Makefile.** The recently-closed read-path race would have been caught by one `go test -race` invocation in CI; the next Tier-0 regression will not be caught by anything.
6. **Repo hygiene:** `cmd/ledger-cli/main.go` still has the uncommitted `rpc-test` command with hardcoded third-party Ethereum endpoints and a `golang.org/x/net/websocket` import that leaked into `go.mod` (`go.sum` carries the transitive deps). `wal.encodeBatch`, `replication.dummyWriter`/`encodeTransfer`/`decodeTransferPayload` are dead code.
3. **Newly-surfaced bugs the previous roadmap missed:**
   - `core.Balance` still `panic`s on underflow — only the *outer* `ApplyDebit` was hardened (C0.7 partial).
   - REST `/v1/transfers` and `/v1/accounts` don't map `core.ErrNotLeader` → 503, so a write against a follower returns generic 500.
   - REST `bytesToID` right-pads short hex IDs, silently aliasing `01` and `00000000000000000000000000000001`.
   - `NewLedger` calls `RecoverFromSnapshot(snapshotLSN)` after loading the snapshot into memory, which triggers a *full* WAL replay even on a snapshot-only path — wasted I/O and a foot-gun for future record types.
   - `peekSegmentMaxLSN` reads entire candidate segments into memory — every `TruncateBefore` call scans the whole dir.
   - `Batcher` + read/create queues are drained from the same `select` with `default:` → read latency is unbounded under heavy write load (no timer fairness).
   - `GetAccount` on the API allocates a fresh `chan ReadAccountResult` per request; combined with `TransferEvent` allocating `chan error` per submit, allocation pressure is the *first* thing to fix on the way to D1.1.
4. **Still unwired decoration:** `Checkpointer` exists but is never started in `cmd/server`; `events.Publisher` has no caller; `observability.RingBufferDepth` gauge is declared but never `.Set()`-ed; `deploy/prometheus.yml` scrapes `aequitas-ledger:6060` which matches no compose service; `deploy/k8s/deployment-follower.yaml` has no `volumes`/`volumeMounts`; `docs/` is untracked in git while `Status.md` claims "ADRs complete and committed."
5. **No CI, no lint, no `-race` enforcement, no Makefile.** The recently-closed read-path race would have been caught by one `go test -race` invocation in CI; the next Tier-0 regression will not be caught by anything.
6. **Repo hygiene:** `cmd/ledger-cli/main.go` still has the uncommitted `rpc-test` command with hardcoded third-party Ethereum endpoints and a `golang.org/x/net/websocket` import that leaked into `go.mod` (`go.sum` carries the transitive deps). `wal.encodeBatch`, `replication.dummyWriter`/`encodeTransfer`/`decodeTransferPayload` are dead code.

### The TigerBeetle gap, summarized

| Dimension | TigerBeetle | aequitas today |
|---|---|---|
| API model | Batched: ~8K transfers per message, array of per-item results | Unary, one `chan error` per transfer |
| Transfer lifecycle | Two-phase: pending → post/void, timeout, linked chains | Immediate post only |
| Schema | ledger, code, user_data_128/64, flags semantics | id/currency/balances + frozen/closed only |
| Read path | Query API with filters + cursor pagination | Transfers are write-only (no read path at all); accounts readable via loop hop |
| Read path | Query API with filters + cursor pagination | Transfers are write-only (no read path at all); accounts readable via loop hop |
| Storage | Preallocated static files, direct I/O, io_uring, own page cache, no full-WAL replay at startup | Buffered `WriteAt` per record, whole segments read into memory on recovery, full replay |
| Correctness testing | VOPR deterministic fault-injection simulator, run continuously in CI | Happy-path + single-point crash + small race suite (no fault injection, no byte-level truncation sweep) |
| Replication | Quorum-replicated state machine, epochs, fencing | Best-effort async TCP stream, per-record `AppendBatch` on follower, no acks/quorum/fencing/term |
| Correctness testing | VOPR deterministic fault-injection simulator, run continuously in CI | Happy-path + single-point crash + small race suite (no fault injection, no byte-level truncation sweep) |
| Replication | Quorum-replicated state machine, epochs, fencing | Best-effort async TCP stream, per-record `AppendBatch` on follower, no acks/quorum/fencing/term |
| Throughput | ~1M TPS class | ~2,900 TPS |

---

## Tier 0 — Correctness debt (do this before anything else)

> **Guiding rule:** a ledger earns trust by what it refuses to get wrong. Every item here is observable by a reviewer.
> **Guiding rule:** a ledger earns trust by what it refuses to get wrong. Every item here is observable by a reviewer.

### C0.1 — Read-path data race
- **Status:** ✅ Done
- **Resolution:** `Ledger.GetAccount` and `Ledger.GetBalance` hop through `loop.readQueue` (`internal/engine/ledger.go:198-222`); the loop drains the queue and returns a value copy (`loop.go:57-63`).
- **Proof:** `tests/integration/race_test.go::TestConcurrentReadWriteRace` runs under `go test -race ./...` clean. Concurrent writers + 4 readers + a creator goroutine for 3 seconds.
- **Follow-up (carry into ADR):** document the loop-hop design in an ADR (seqlock vs loop-hop) so future readers don't "optimize" reads back into a direct map access.
- **Status:** ✅ Done
- **Resolution:** `Ledger.GetAccount` and `Ledger.GetBalance` hop through `loop.readQueue` (`internal/engine/ledger.go:198-222`); the loop drains the queue and returns a value copy (`loop.go:57-63`).
- **Proof:** `tests/integration/race_test.go::TestConcurrentReadWriteRace` runs under `go test -race ./...` clean. Concurrent writers + 4 readers + a creator goroutine for 3 seconds.
- **Follow-up (carry into ADR):** document the loop-hop design in an ADR (seqlock vs loop-hop) so future readers don't "optimize" reads back into a direct map access.

### C0.2 — Non-durable accounts
- **Status:** ✅ Done
- **Resolution:** `processAccountCreate` writes a `RecordTypeAccount` + `Sync()` and only then mutates in-memory state (`internal/engine/loop.go:77-96`); `RecoverFromLSN` replays both `RecordTypeAccount` and `RecordTypeTransfer` (`ledger.go:247-279`).
- **Proof:** `tests/integration/account_durability_test.go::TestAccountDurabilityAcrossCrash` creates accounts via API, performs a transfer, closes the ledger, reopens with no `InitialAccounts`, and asserts the balances.
### C0.2 — Non-durable accounts
- **Status:** ✅ Done
- **Resolution:** `processAccountCreate` writes a `RecordTypeAccount` + `Sync()` and only then mutates in-memory state (`internal/engine/loop.go:77-96`); `RecoverFromLSN` replays both `RecordTypeAccount` and `RecordTypeTransfer` (`ledger.go:247-279`).
- **Proof:** `tests/integration/account_durability_test.go::TestAccountDurabilityAcrossCrash` creates accounts via API, performs a transfer, closes the ledger, reopens with no `InitialAccounts`, and asserts the balances.

### C0.3 — Broken LSN (Log Sequence Number) accounting
- **Status:** ✅ Done
- **Resolution:** LSN is assigned per-record in `wal.AppendBatch` (`internal/wal/wal.go:62-91`); `BatchCommit` records carry their own LSN; `TruncateBefore(lsn)` now skips any segment whose `maxLSN >= lsn` (`wal.go:155-172`); `peekSegmentMaxLSN` is segment-aware.
- **Proof:** `tests/unit/wal_test.go::TestWALAppendAndRecover` (1000-record round-trip in LSN order) and `TestWALRecoverTruncatedTail` (5-byte tail truncation, recover yields n−1 records) pass.
### C0.3 — Broken LSN (Log Sequence Number) accounting
- **Status:** ✅ Done
- **Resolution:** LSN is assigned per-record in `wal.AppendBatch` (`internal/wal/wal.go:62-91`); `BatchCommit` records carry their own LSN; `TruncateBefore(lsn)` now skips any segment whose `maxLSN >= lsn` (`wal.go:155-172`); `peekSegmentMaxLSN` is segment-aware.
- **Proof:** `tests/unit/wal_test.go::TestWALAppendAndRecover` (1000-record round-trip in LSN order) and `TestWALRecoverTruncatedTail` (5-byte tail truncation, recover yields n−1 records) pass.

### C0.4 — Torn-batch replay
- **Status:** ✅ Done
- **Resolution:** `wal.AppendBatch` now appends a `RecordTypeBatchCommit` marker after the user records (`wal.go:93-114`); `replaySegment` buffers records into `pendingBatch` and only invokes the handler after a matching commit marker (`recovery.go:81-128`); a partial batch at EOF is truncated.
- **Proof:** `tests/unit/wal_test.go::TestWALRecoverTruncatedTail` covers a torn tail. **Gap:** no test currently truncates mid-batch (between record N and N+1 of a multi-record batch). Add that as part of T3.1.
### C0.4 — Torn-batch replay
- **Status:** ✅ Done
- **Resolution:** `wal.AppendBatch` now appends a `RecordTypeBatchCommit` marker after the user records (`wal.go:93-114`); `replaySegment` buffers records into `pendingBatch` and only invokes the handler after a matching commit marker (`recovery.go:81-128`); a partial batch at EOF is truncated.
- **Proof:** `tests/unit/wal_test.go::TestWALRecoverTruncatedTail` covers a torn tail. **Gap:** no test currently truncates mid-batch (between record N and N+1 of a multi-record batch). Add that as part of T3.1.

### C0.5 — Snapshot subsystem unwired
- **Status:** 🟨 Partially done (path exists, not started in production)
- **Problem:** `Ledger.TriggerSnapshot` + `RecoverFromSnapshot` work and `TestSnapshotRecoveryAndTruncation` passes, but the *background* `Checkpointer` (`internal/snapshot/checkpointer.go`) is never started by `cmd/server`. Snapshots are taken only by manual trigger.
- **Evidence:** `grep -rn "NewCheckpointer" --include="*.go"` → no production caller. `cmd/server/main.go` does not import `internal/snapshot`.
- **Fix:**
  1. In `cmd/server/main.go`, after the engine is initialized, start `snapshot.NewCheckpointer(...)` with `cfg.SnapshotInterval` and `cfg.MaxSnapshotsKept`.
  2. Wait for the initial snapshot LSN to be ≥ the recovered LSN before declaring "ready" (readiness probe).
  3. Add a `Ledger.WaitForCaughtUp()` if/when a follower mode is added (out of scope for C0.5 but reserved).
- **Proof:** New integration test: start server with `SNAPSHOT_INTERVAL=200ms`; submit 1k transfers; assert that ≥1 snapshot file exists in `SNAPSHOT_DIR` and that segments strictly older than the latest snapshot are deleted; restart and assert identical balances and a recovery-time metric reported.

### C0.6 — Non-durable idempotency
- **Status:** ✅ Done
- **Resolution:** `RecoverFromLSN` calls `l.idempKey.Commit(t.IdempotencyKey, t)` for each replayed transfer with a non-zero key (`internal/engine/ledger.go:272-274`).
- **Proof:** `tests/integration/idempotency_durability_test.go::TestIdempotencyDurabilityAcrossRestart` — submit transfer with key K → kill → restart → retry K with a *different* transfer ID → returns the original transfer result and the balance is debited exactly once.
### C0.5 — Snapshot subsystem unwired
- **Status:** 🟨 Partially done (path exists, not started in production)
- **Problem:** `Ledger.TriggerSnapshot` + `RecoverFromSnapshot` work and `TestSnapshotRecoveryAndTruncation` passes, but the *background* `Checkpointer` (`internal/snapshot/checkpointer.go`) is never started by `cmd/server`. Snapshots are taken only by manual trigger.
- **Evidence:** `grep -rn "NewCheckpointer" --include="*.go"` → no production caller. `cmd/server/main.go` does not import `internal/snapshot`.
- **Fix:**
  1. In `cmd/server/main.go`, after the engine is initialized, start `snapshot.NewCheckpointer(...)` with `cfg.SnapshotInterval` and `cfg.MaxSnapshotsKept`.
  2. Wait for the initial snapshot LSN to be ≥ the recovered LSN before declaring "ready" (readiness probe).
  3. Add a `Ledger.WaitForCaughtUp()` if/when a follower mode is added (out of scope for C0.5 but reserved).
- **Proof:** New integration test: start server with `SNAPSHOT_INTERVAL=200ms`; submit 1k transfers; assert that ≥1 snapshot file exists in `SNAPSHOT_DIR` and that segments strictly older than the latest snapshot are deleted; restart and assert identical balances and a recovery-time metric reported.

### C0.6 — Non-durable idempotency
- **Status:** ✅ Done
- **Resolution:** `RecoverFromLSN` calls `l.idempKey.Commit(t.IdempotencyKey, t)` for each replayed transfer with a non-zero key (`internal/engine/ledger.go:272-274`).
- **Proof:** `tests/integration/idempotency_durability_test.go::TestIdempotencyDurabilityAcrossRestart` — submit transfer with key K → kill → restart → retry K with a *different* transfer ID → returns the original transfer result and the balance is debited exactly once.

### C0.7 — Panics inside the state machine
- **Status:** 🟨 Partially done
- **What changed:** `ApplyDebit`/`ApplyCredit` now return typed errors (C0.7a). The `panic` in `core.Balance` (line 22 of `internal/core/account.go`) is still there, and is reachable on `GetAccount` if a `Uint128` underflow ever leaks through. `ring_buffer.go:24` panics on bad size — fine at construction, but still a `panic(` that an honest grep gate would flag.
- **Fix:**
  1. Replace `core.Balance`'s `panic` with `core.ErrInvariantViolation`; the event loop must `metrics.Inc("invariant_violations")` and either (a) refuse to apply and fail the batch with a typed error, or (b) on a single-writer system, `os.Exit(2)` after the metric is set. Document the chosen policy in an ADR.
  2. Add an explicit `assertInvariant(metric *Metrics)` at the end of `ApplyBatch`; make it tested.
  3. Move the `NewRingBuffer` size check from `panic` to `error` so callers can fail fast.
  4. Add a CI grep gate: `! grep -RnE "panic\\(" internal/engine internal/core internal/wal internal/replication`.
- **Proof:** Unit tests for underflow/overflow/missing-account paths returning typed errors; invariant-violation test that asserts the metric increments; grep gate is a hard CI check.
- **Status:** 🟨 Partially done
- **What changed:** `ApplyDebit`/`ApplyCredit` now return typed errors (C0.7a). The `panic` in `core.Balance` (line 22 of `internal/core/account.go`) is still there, and is reachable on `GetAccount` if a `Uint128` underflow ever leaks through. `ring_buffer.go:24` panics on bad size — fine at construction, but still a `panic(` that an honest grep gate would flag.
- **Fix:**
  1. Replace `core.Balance`'s `panic` with `core.ErrInvariantViolation`; the event loop must `metrics.Inc("invariant_violations")` and either (a) refuse to apply and fail the batch with a typed error, or (b) on a single-writer system, `os.Exit(2)` after the metric is set. Document the chosen policy in an ADR.
  2. Add an explicit `assertInvariant(metric *Metrics)` at the end of `ApplyBatch`; make it tested.
  3. Move the `NewRingBuffer` size check from `panic` to `error` so callers can fail fast.
  4. Add a CI grep gate: `! grep -RnE "panic\\(" internal/engine internal/core internal/wal internal/replication`.
- **Proof:** Unit tests for underflow/overflow/missing-account paths returning typed errors; invariant-violation test that asserts the metric increments; grep gate is a hard CI check.

### C0.8 — Unsound replication apply
- **Status:** 🟨 Partially done
- **What changed:** The follower no longer auto-creates zero-balance accounts (`internal/replication/follower.go:184-189` returns an error on missing account), and the wire frame has CRC32 (`follower.go:138-142`, `replication/server.go:94-99`).
- **What remains:**
  1. **Per-record `AppendBatch` on the follower.** `follower.go:147-154` calls `f.localWAL.AppendBatch([]wal.Record{rec})` for *every* received record. This bypasses group commit on the follower side, writes a `BatchCommit` for each one (because `AppendBatch` always appends a commit), and is the antithesis of group commit. Refactor so that the follower buffers N records and writes them as a single `AppendBatch`, or add a `wal.AppendOne`/`wal.Append` that does not require a commit marker for single-record appends.
  2. **Reconnect storm on bad CRC.** On CRC mismatch `follower.go:140-142` returns an error and `syncLoop` retries with `time.Sleep(100ms)` (`follower.go:55-58`). A single corrupt frame causes 10 reconnects/sec, each of which re-`Dial`s TCP and re-`RecoverFromLSN` — a malicious corrupt-frame storm becomes a self-DoS. Add exponential backoff (start 100ms, cap 5s) and a hard circuit-breaker after K consecutive failures that triggers a full re-sync.
  3. **No leader epoch / fencing.** A stale primary that comes back to life can keep streaming to a follower while a *new* primary is also streaming. Add an 8-byte `term` (u64) at the start of the wire frame; follower rejects frames with `term < lastTerm` and primary bumps `term` on every promotion; on rejection, follower drops state and triggers full re-sync.
  4. **Server-side `RecoverFromLSN` re-scans the entire WAL for every reconnect.** `replication/server.go:85-105` calls `w.RecoverFromLSN` inside a `for` loop on the connection handler — this rebuilds the recovery state from scratch on every connection. Add an in-memory cursor that tracks `lastStreamedLSN` per follower, advances it as frames are written, and lets the next reconnect request only the range `[lastStreamedLSN+1, currentLSN]`.
  5. **Apply through the engine's own code path.** Right now the follower decodes the payload and calls `accounts.ApplyDebit`/`ApplyCredit` directly. A "trusted replay" mode that calls the same validation+apply path as `RecoverFromLSN` would guarantee byte-identical state machines.
- **Proof:** New tests:
  - corrupted frame → follower drops the connection, sleeps with exponential backoff, then attempts a *full re-sync* (verified by `LastLSN` reset and full re-replay).
  - stale-term frame → follower rejects without mutating state.
  - 10k-transfer primary → follower (with the new batched apply) → balances match exactly; follower WAL has 1 `BatchCommit` per ~N records, not 1 per record.

### C0.9 — Server cannot actually run as a follower
- **Status:** ✅ Done (with one operational gap)
- **What changed:** `cmd/server/main.go:40,52-65` reads `LEDGER_ROLE`; `IsFollower` is enforced in `CreateTransfer`/`CreateAccount`; `TestFollowerModeRejectsWrites` and `TestReplicationPrimaryToFollower` pass.
- **Operational gap:** `deploy/k8s/deployment-follower.yaml` has no `volumes`/`volumeMounts` for the follower's WAL directory. On a pod restart the follower loses its local WAL and the next reconnect requires a full resync. Add the same `volumeClaimTemplate` pattern as the primary's `statefulset-primary.yaml` (or a PVC for the Deployment).
- **Proof:** `kustomize build` / `kubectl apply` against the manifests results in both pods having a WAL volume; a follower-pod restart does not lose replication state.
### C0.8 — Unsound replication apply
- **Status:** 🟨 Partially done
- **What changed:** The follower no longer auto-creates zero-balance accounts (`internal/replication/follower.go:184-189` returns an error on missing account), and the wire frame has CRC32 (`follower.go:138-142`, `replication/server.go:94-99`).
- **What remains:**
  1. **Per-record `AppendBatch` on the follower.** `follower.go:147-154` calls `f.localWAL.AppendBatch([]wal.Record{rec})` for *every* received record. This bypasses group commit on the follower side, writes a `BatchCommit` for each one (because `AppendBatch` always appends a commit), and is the antithesis of group commit. Refactor so that the follower buffers N records and writes them as a single `AppendBatch`, or add a `wal.AppendOne`/`wal.Append` that does not require a commit marker for single-record appends.
  2. **Reconnect storm on bad CRC.** On CRC mismatch `follower.go:140-142` returns an error and `syncLoop` retries with `time.Sleep(100ms)` (`follower.go:55-58`). A single corrupt frame causes 10 reconnects/sec, each of which re-`Dial`s TCP and re-`RecoverFromLSN` — a malicious corrupt-frame storm becomes a self-DoS. Add exponential backoff (start 100ms, cap 5s) and a hard circuit-breaker after K consecutive failures that triggers a full re-sync.
  3. **No leader epoch / fencing.** A stale primary that comes back to life can keep streaming to a follower while a *new* primary is also streaming. Add an 8-byte `term` (u64) at the start of the wire frame; follower rejects frames with `term < lastTerm` and primary bumps `term` on every promotion; on rejection, follower drops state and triggers full re-sync.
  4. **Server-side `RecoverFromLSN` re-scans the entire WAL for every reconnect.** `replication/server.go:85-105` calls `w.RecoverFromLSN` inside a `for` loop on the connection handler — this rebuilds the recovery state from scratch on every connection. Add an in-memory cursor that tracks `lastStreamedLSN` per follower, advances it as frames are written, and lets the next reconnect request only the range `[lastStreamedLSN+1, currentLSN]`.
  5. **Apply through the engine's own code path.** Right now the follower decodes the payload and calls `accounts.ApplyDebit`/`ApplyCredit` directly. A "trusted replay" mode that calls the same validation+apply path as `RecoverFromLSN` would guarantee byte-identical state machines.
- **Proof:** New tests:
  - corrupted frame → follower drops the connection, sleeps with exponential backoff, then attempts a *full re-sync* (verified by `LastLSN` reset and full re-replay).
  - stale-term frame → follower rejects without mutating state.
  - 10k-transfer primary → follower (with the new batched apply) → balances match exactly; follower WAL has 1 `BatchCommit` per ~N records, not 1 per record.

### C0.9 — Server cannot actually run as a follower
- **Status:** ✅ Done (with one operational gap)
- **What changed:** `cmd/server/main.go:40,52-65` reads `LEDGER_ROLE`; `IsFollower` is enforced in `CreateTransfer`/`CreateAccount`; `TestFollowerModeRejectsWrites` and `TestReplicationPrimaryToFollower` pass.
- **Operational gap:** `deploy/k8s/deployment-follower.yaml` has no `volumes`/`volumeMounts` for the follower's WAL directory. On a pod restart the follower loses its local WAL and the next reconnect requires a full resync. Add the same `volumeClaimTemplate` pattern as the primary's `statefulset-primary.yaml` (or a PVC for the Deployment).
- **Proof:** `kustomize build` / `kubectl apply` against the manifests results in both pods having a WAL volume; a follower-pod restart does not lose replication state.

### C0.10 — Housekeeping / dead code / hygiene
- **Status:** ⬜ Not started
- **Status:** ⬜ Not started
- **Problem:** Dead and unrelated code erodes reviewer trust faster than missing features.
- **Items (each a single commit):**
  1. **Remove `cmd/ledger-cli/rpc-test` and `golang.org/x/net/websocket`.** The `rpc-test` command (`cmd/ledger-cli/main.go:63-65,72-147`) hits a hardcoded third-party endpoint. Strip it; drop the `websocket` import; `go mod tidy`.
  2. **Delete dead code:**
     - `internal/wal/wal.go:290-303` — `encodeBatch` (superseded by `AppendBatch`).
     - `internal/replication/follower.go:199-243` — `decodeTransferPayload` (duplicate of `engine.DecodeTransferPayload`), `encodeTransfer`, and `dummyWriter` (unreferenced).
     - `internal/core/errors.go:74-78` — `ErrDuplicateTransferID` (defined, never produced, never checked; either enforce it in `ApplyBatch` by checking a transfer-ID index, or delete).
     - `internal/observability/metrics.go:49-56` — `RingBufferDepth` gauge: either set it in the loop on every batch (`observability.RingBufferDepth.Set(float64(l.rb.Len()))`) or delete it.
     - `internal/events/publisher.go` — `Publisher`: wire the engine to publish on each committed batch (outbox is the natural seam; see T3.4) or delete.
  3. **Commit the `docs/` tree.** `git status` shows `?? docs/`. `git add docs/ && git commit -m "docs: track docs/ tree"`. Update `Status.md` to no longer say "ADRs complete and committed" *before* the commit lands, or amend afterwards.
  4. **Fix `deploy/prometheus.yml:7`.** Targets `aequitas-ledger:6060`; compose services are `ledger-primary` and `ledger-follower`. Replace with `['ledger-primary:6060', 'ledger-follower:6060']`.
  5. **Fix the case-sensitivity in `docker-compose.yml`.** `LEDGER_ROLE=FOLLOWER` works only because the read is case-insensitive by accident; normalize via `strings.ToLower` in `config.Load` and document.
  6. **Update the README claims table.** `README.md:5` claims "200k–500k TPS" while the bench report measures 2,900 TPS. Add a "(design target, not yet measured — see Tier 2 / S2.4)" annotation.
- **Proof:** `golangci-lint --disable-all --enable=unused,deadcode,gosimple ./...` clean; `git status` clean; `go mod tidy` reports no changes; `grep -RnE "panic\\(" internal/engine internal/core internal/wal internal/replication` returns nothing (C0.7 + C0.10 jointly).

---

### Tier 0 — Newly surfaced items (not in previous roadmap)

### C0.11 — REST gateway does not map `ErrNotLeader` correctly
- **Problem:** `internal/api/rest.go:213-228` and `rest.go:96-105` only handle `ErrZeroAmount`/`ErrSelfTransfer`/`ErrAccountNotFound`/`ErrInsufficientFunds`; `core.ErrNotLeader` falls through to a generic 500. The gRPC handlers in `internal/api/server.go:51,134` do map it (to `codes.Unavailable`), so behavior is inconsistent.
- **Evidence:** `internal/api/rest.go:96-105` (`handleAccounts`) and `rest.go:213-228` (`handleCreateTransfer`).
- **Fix:** Add `if errors.Is(err, core.ErrNotLeader{}) { writeError(w, http.StatusServiceUnavailable, err.Error()); return }` in both REST handlers.
- **Proof:** Unit test that points the REST gateway at a follower-mode ledger and asserts 503 on POST.

### C0.12 — REST `bytesToID` silently aliases short IDs
- **Problem:** `internal/api/rest.go:241-253` and `internal/api/server.go:162-172` use `copy(id[16-len(b):], b)`. An ID `"0x01"` and an ID `"00000000000000000000000000000001"` both produce `[0,...,0,1]`. They are the same account, but the user does not know.
- **Fix:** Reject any ID with `len(b) < 16` (`errors.New("id must be exactly 16 bytes (32 hex chars)")`). Document in REST API docs.
- **Proof:** Unit test for `parseHexID("01")` → error; `parseHexID("00000000000000000000000000000001")` → 16-byte id.

### C0.13 — `NewLedger` re-replays the WAL after loading the snapshot
- **Problem:** `internal/engine/ledger.go:83-101` loads the latest snapshot into `accounts` (mutating state), then calls `RecoverFromSnapshot(snapshotLSN)` which does `wal.RecoverFromLSN(snapshotLSN, ...)` — replaying from the snapshot LSN. Correctness-wise this is fine (Create is idempotent on duplicate ID, ApplyBatch is idempotent on the same transfer). But: (a) it is wasted I/O; (b) it depends on `Create`/`ApplyBatch` *remaining* idempotent, which is an undocumented invariant; (c) it makes "what does a snapshot mean?" ambiguous.
- **Fix:** Either (a) load the snapshot *into* the WAL recovery path — i.e., make recovery itself snapshot-aware, or (b) make snapshot load truly authoritative: skip replay entirely and assert `wal.RecoverFromLSN` is called only when the snapshot LSN is 0. Pick (b); it's smaller.
- **Proof:** Test: write 100 transfers → snapshot at LSN 100 → restart → assert the recovered LSN is exactly 100 (no replay of records 1–100) and balances are correct.

### C0.14 — `peekSegmentMaxLSN` reads whole segments into RAM on every `TruncateBefore`
- **Problem:** `internal/wal/wal.go:174-208` does `os.ReadFile(path)` for every non-active segment on every `TruncateBefore` call. With 64 MiB segments and a snapshot loop firing every 60 s, this is 64 MiB of I/O per minute per non-active segment, forever, until they're deleted. Also called out in S2.3 for recovery; the same fix applies.
- **Fix:** Walk the segment tail-first (last record header is `<=` 13 B + 4 B CRC from EOF) and read only the last frame; if malformed, binary-search backwards.
- **Proof:** Benchmark `TruncateBefore` against a directory with 100× 64 MiB segments; assert peak RSS < 1 MiB and latency < 50 ms.

### C0.15 — `GetAccount` / `CreateAccount` allocate a new channel per request
- **Problem:** `internal/engine/ledger.go:178-179,199-200` allocate `chan error` and `chan ReadAccountResult` per request. On a 200k-TPS target this is 200k channel allocations/sec. The single biggest allocation source in the hot path.
- **Fix (Tier 0 because allocation pressure *is* a correctness concern at the throughput this code aspires to):** Use a `sync.Pool` of channels, or — better — switch to a one-shot response struct + buffered channel reused from a per-handler pool. Companion fix in `event.go:5-12` (`TransferEvent` allocates a fresh `chan error` per submit).
- **Proof:** `b.ReportAllocs()` on the bench harness shows allocs/op dropping to 0 for the read and submit paths.

### C0.16 — Batcher / read-queue drain is unfair
- **Problem:** `internal/engine/loop.go:49-75` uses a single `select` with `default:` that drains `snapshotRequests`, `readQueue`, `accountCreateQueue` and then unconditionally calls `Batcher.Collect()`. Under heavy write load, `Batcher.Collect()` returns instantly (ring buffer non-empty), so the `default:` branch is never taken, and reads/creates can be starved indefinitely. Read latency is unbounded under load.
- **Fix:** Add a `time.Timer` (or a tick on every N iterations) that drains the read/create queues at least every 1 ms; or move reads/creates onto a goroutine that uses `select{case l.loop.readQueue: ...; case <-tick.C: ...}`.
- **Proof:** New test: 1 writer goroutine submitting 10k transfers; 1 reader goroutine calling `GetAccount`; assert p99 read latency < 5 ms.

---

## Tier 1 — TigerBeetle domain depth (the "inspiration" delta)

These are what make TigerBeetle a *financial* database rather than a fast KV with balances. D1.1 is the single biggest TPS lever; without it, Tier 2 is noise.
These are what make TigerBeetle a *financial* database rather than a fast KV with balances. D1.1 is the single biggest TPS lever; without it, Tier 2 is noise.

### D1.1 — Batched request/response protocol *(single biggest TPS lever)*
- Replace unary `CreateTransfer` with `CreateTransfers([]Transfer) -> []Result` (batch up to ~1K–8K items per RPC), same for accounts. Eliminates per-transfer `chan error` allocation (C0.15 becomes moot), amortizes API overhead, and is how TigerBeetle hits its throughput.
- Internals: one submit, one result slice, one completion notification per batch. The proto already has `repeated` for most fields, so this is a *handler* refactor more than a schema change.
- REST equivalent: accept arrays. Per-item status; batch accepted atomically at the protocol level (mirrors TigerBeetle).
- **Proof:** Bench: batch-8192 RPC ≥ 25× unary TPS; allocs/op on submit path near zero. Capture pprof before/after.
- Replace unary `CreateTransfer` with `CreateTransfers([]Transfer) -> []Result` (batch up to ~1K–8K items per RPC), same for accounts. Eliminates per-transfer `chan error` allocation (C0.15 becomes moot), amortizes API overhead, and is how TigerBeetle hits its throughput.
- Internals: one submit, one result slice, one completion notification per batch. The proto already has `repeated` for most fields, so this is a *handler* refactor more than a schema change.
- REST equivalent: accept arrays. Per-item status; batch accepted atomically at the protocol level (mirrors TigerBeetle).
- **Proof:** Bench: batch-8192 RPC ≥ 25× unary TPS; allocs/op on submit path near zero. Capture pprof before/after.

### D1.2 — Two-phase transfers (pending → post/void)
- Add `PendingDebits/PendingCredits` to accounts (proto already reserves these fields), `timeout` on transfers, and `post_pending_transfer` / `void_pending_transfer` flags. Models holds/authorizations — the reason payment systems look the way they do.
- **Proof:** Unit tests for hold-expiry (logical clock, not wall clock), post/void idempotency, insufficient-available-funds vs posted balance.
- Add `PendingDebits/PendingCredits` to accounts (proto already reserves these fields), `timeout` on transfers, and `post_pending_transfer` / `void_pending_transfer` flags. Models holds/authorizations — the reason payment systems look the way they do.
- **Proof:** Unit tests for hold-expiry (logical clock, not wall clock), post/void idempotency, insufficient-available-funds vs posted balance.

### D1.3 — Linked transfers (chains)
- `flag_linked` semantics: a batch containing linked transfers commits atomically or fails as a chain. Two-phase across accounts (escrow patterns: debit A/credit escrow + escrow/debit B/credit B in one atomic chain).
- **Proof:** Chain-failure test: link of 3 where #2 fails → nothing applied. Chain-success test: 3-link atomic move of funds.
- `flag_linked` semantics: a batch containing linked transfers commits atomically or fails as a chain. Two-phase across accounts (escrow patterns: debit A/credit escrow + escrow/debit B/credit B in one atomic chain).
- **Proof:** Chain-failure test: link of 3 where #2 fails → nothing applied. Chain-success test: 3-link atomic move of funds.

### D1.4 — Rich schema: ledger, code, user_data
- `ledger u32` (multi-tenant isolation; invariant becomes per-ledger), `code u16` (transfer type / account type), `user_data_128`/`user_data_64` (opaque correlation fields the DB promises to store and return, never interpret). Cheap now, impossible later without a migration story.
- **Proof:** Invariant checker scope becomes per-ledger; API round-trips `user_data` byte-for-byte; invariant test runs across 2 ledgers and asserts cross-ledger conservation does *not* hold (they are separate books).
- `ledger u32` (multi-tenant isolation; invariant becomes per-ledger), `code u16` (transfer type / account type), `user_data_128`/`user_data_64` (opaque correlation fields the DB promises to store and return, never interpret). Cheap now, impossible later without a migration story.
- **Proof:** Invariant checker scope becomes per-ledger; API round-trips `user_data` byte-for-byte; invariant test runs across 2 ledgers and asserts cross-ledger conservation does *not* hold (they are separate books).

### D1.5 — Query APIs (there is currently NO transfer read path)
- `GetTransfers` with filters (account_id, code, ledger, time range, flags) + cursor pagination. Requires a transfer history index — design decision for an ADR: per-account append-only log of transfer IDs + WAL-offset index vs periodic sorted on-disk segments.
- `GetAccountTransfers`, `GetAccountBalances` (time-travel is a stretch goal).
- **Proof:** Integration test paginates a 100k-transfer history with a cursor; no unbounded memory use; cursor round-trips.
- **Proof:** Integration test paginates a 100k-transfer history with a cursor; no unbounded memory use; cursor round-trips.

### D1.6 — Timestamp discipline
- Batch-synchronous timestamps (one per batch, exposed to clients) rather than `time.Now()` inside the loop. Recovery already preserves original timestamps; test that explicitly.
- **Proof:** Test: submit a batch → record `before := time.Now()`; recover → assert `t.Timestamp` is between `before` and `time.Now()` *and* is identical across replay.
- Batch-synchronous timestamps (one per batch, exposed to clients) rather than `time.Now()` inside the loop. Recovery already preserves original timestamps; test that explicitly.
- **Proof:** Test: submit a batch → record `before := time.Now()`; recover → assert `t.Timestamp` is between `before` and `time.Now()` *and* is identical across replay.

---

## Tier 2 — Storage engine & performance (earn the 200k TPS claim or retract it)

Current cost structure per transfer: unary RPC + per-transfer channel alloc + `WriteAt` per record + small effective batches + busy-spin loops. Staged plan:

### S2.1 — Make the write path cheap before making it clever
- Encode the whole batch into one buffer; one sequential `Write` (append), not per-record `WriteAt`. Right now `wal.AppendBatch` calls `current.Write(encoded)` per record; a single `bytes.Buffer` + one `Write` is strictly better.
- Encode the whole batch into one buffer; one sequential `Write` (append), not per-record `WriteAt`. Right now `wal.AppendBatch` calls `current.Write(encoded)` per record; a single `bytes.Buffer` + one `Write` is strictly better.
- `fdatasync` on Linux (cheaper than `fsync`; macOS fallback).
- Right-size batching: with unary RPCs the batcher rarely sees >a few events; fix D1.1 first or the rest is noise.
- Remove busy-spin: block the loop on a notify channel + timer for `BatchTimeout` instead of `default:` + `Gosched` spin (`loop.go:45-61`, `batch.go`). Idle CPU should be ~0.
- **Proof:** pprof flame graphs before/after committed to the bench report; idle CPU < 1%; `Gosched` removed from hot path.
- **Proof:** pprof flame graphs before/after committed to the bench report; idle CPU < 1%; `Gosched` removed from hot path.

### S2.2 — File preallocation + direct I/O path
- `fallocate` segments at creation (no filesystem metadata churn mid-write, no fragmentation). Linux `O_DIRECT` with aligned buffers and a write-through path; keep buffered fallback for macOS/dev. TigerBeetle's core bet is that a database should own its I/O.
- `fallocate` segments at creation (no filesystem metadata churn mid-write, no fragmentation). Linux `O_DIRECT` with aligned buffers and a write-through path; keep buffered fallback for macOS/dev. TigerBeetle's core bet is that a database should own its I/O.
- **Proof:** Bench on Linux showing p99 sync-latency stability under sustained load (this is the honest motivation: tail latency, not mean TPS).

### S2.3 — Recovery that doesn't read whole segments into RAM
- Stream records with a bounded buffer instead of `make([]byte, fileSize)` in `replaySegment` (`recovery.go:74-77`) and `peekSegmentMaxLSN` (`wal.go:174-208` — see C0.14). Combined with C0.5 snapshots, startup becomes O(WAL since snapshot) not O(all history).
- **Proof:** Restart with 10 GB WAL: RSS stays bounded (< 50 MiB); recovery-time metric reported.
- Stream records with a bounded buffer instead of `make([]byte, fileSize)` in `replaySegment` (`recovery.go:74-77`) and `peekSegmentMaxLSN` (`wal.go:174-208` — see C0.14). Combined with C0.5 snapshots, startup becomes O(WAL since snapshot) not O(all history).
- **Proof:** Restart with 10 GB WAL: RSS stays bounded (< 50 MiB); recovery-time metric reported.

### S2.4 — The honest performance campaign (rewritten `benchmark_report.md`)
- Staged targets with evidence at each stage: 10k → 50k → 100k+ TPS on your hardware, each with a profile identifying the then-current bottleneck. If a stage stalls, the report says why and what the structural limit is — *that* is principal-level content. Include: TPS at batch sizes 1/128/1024/8192, p50/p95/p99/fsync distribution, hot-account contention scenario, and recovery time vs WAL size.
- Retract or requalify the 200k–500k README claim until measured. Nothing in the repo is more damaging than a claim the numbers contradict.
- **Proof:** A `benchmark_report.md` that shows the *analysis*, not the peak number. Each "we tried X" section has a profile attached.
- **Proof:** A `benchmark_report.md` that shows the *analysis*, not the peak number. Each "we tried X" section has a profile attached.

---

## Tier 3 — Verification: test like a database, not a web app

This is TigerBeetle's actual crown jewel (VOPR). A Go-scale version is very achievable and is the highest credibility-per-effort work available.

### T3.1 — Crash-at-every-LSN property test
- For a seeded workload: for each LSN (or each byte offset — stronger), truncate the WAL there, recover, assert invariants and expected balances (recomputed by an independent model). Run as a normal `go test` with a bounded corpus + a long-run mode for CI nightly.
- **Proof:** Coverage report shows byte-level truncation paths exercised; nightly CI shows zero invariant violations in 10M truncations.

- **Proof:** Coverage report shows byte-level truncation paths exercised; nightly CI shows zero invariant violations in 10M truncations.

### T3.2 — Fault-injecting WAL wrapper
- A `filesystem` interface in front of the WAL: inject bit flips after write, torn writes, short reads, `ENOSPC`, reordered barriers pre-fsync. Every injected fault must end in either a clean recovery or a loud, typed failure — never a silent divergence.
- **Proof:** Property test asserts: for every fault type, recover() returns either nil or a typed error; balances either match expected (computed independently) or the test reports a typed failure with the exact fault.

- **Proof:** Property test asserts: for every fault type, recover() returns either nil or a typed error; balances either match expected (computed independently) or the test reports a typed failure with the exact fault.

### T3.3 — Replication adversarial tests
- Partition/lag/corrupt/duplicate frames between primary and follower (in-process pipe with a chaotic middle). Follower must re-sync or refuse — never diverge. Kill follower mid-sync, promote, verify no lost committed transfers (defines "committed" = fsynced, and later = quorum-replicated).
- **Proof:** Suite covers: primary silent for 5 s, primary returns stale term, frame CRC flipped, frame duplicated, follower killed mid-receive. Each case has an expected outcome.

- **Proof:** Suite covers: primary silent for 5 s, primary returns stale term, frame CRC flipped, frame duplicated, follower killed mid-receive. Each case has an expected outcome.

### T3.4 — First-class invariant auditor
- `ledger-cli audit --wal <dir>`: independent replayer (not the engine's own recovery code) that recomputes balances from raw WAL bytes and verifies conservation + batch atomicity + idempotency-key uniqueness. Shipping the auditor is a strong signal: the system grades itself with code that doesn't share the engine's assumptions.
- This is also where the `events.Publisher` (C0.10) can be wired: the auditor subscribes to a stream of committed batches and re-applies them against a shadow state.
- **Proof:** Auditor catches a planted bug in a test fixture (flip one credit in a batch and assert auditor reports the divergence).

- This is also where the `events.Publisher` (C0.10) can be wired: the auditor subscribes to a stream of committed batches and re-applies them against a shadow state.
- **Proof:** Auditor catches a planted bug in a test fixture (flip one credit in a batch and assert auditor reports the divergence).

### T3.5 — CI enforcement
- `go test -race ./...` on every PR (would have caught C0.1 immediately), fuzz targets with time budget in CI, coverage gate on `internal/wal` + `internal/engine` (e.g., 85%+), nightly long-run property tests, grep gate for `panic(` (C0.7 / C0.10).
- **Proof:** A green CI badge that actually means something; the next Tier-0 regression cannot merge.
- `go test -race ./...` on every PR (would have caught C0.1 immediately), fuzz targets with time budget in CI, coverage gate on `internal/wal` + `internal/engine` (e.g., 85%+), nightly long-run property tests, grep gate for `panic(` (C0.7 / C0.10).
- **Proof:** A green CI badge that actually means something; the next Tier-0 regression cannot merge.

---

## Tier 4 — Replication & distribution correctness

### R4.1 — A real replication protocol (before any Raft)
- **Live tailing** (follower stays connected, primary streams new commits) — done in skeleton form; needs cursor + ack.
- **Follower ACKs** → primary tracks per-follower commit index; expose replication lag as a metric and readiness condition.
- **Epoch/term fencing on promotion:** a promoted follower carries a term; stale primaries cannot corrupt (write fencing in the protocol, `ErrNotLeader` with redirect hint on writes). C0.8.3.
- **Follower persists its own WAL** (C0.9 ✅). Add a `LastStreamedLSN` cursor on the primary so reconnects don't rescan the whole WAL (C0.8.4).

- **Live tailing** (follower stays connected, primary streams new commits) — done in skeleton form; needs cursor + ack.
- **Follower ACKs** → primary tracks per-follower commit index; expose replication lag as a metric and readiness condition.
- **Epoch/term fencing on promotion:** a promoted follower carries a term; stale primaries cannot corrupt (write fencing in the protocol, `ErrNotLeader` with redirect hint on writes). C0.8.3.
- **Follower persists its own WAL** (C0.9 ✅). Add a `LastStreamedLSN` cursor on the primary so reconnects don't rescan the whole WAL (C0.8.4).

### R4.2 — Configurable durability
- `--replication=async|quorum`: quorum mode means a batch is ACKed to clients only after majority fsync-ack. The honest latency cost gets measured in the bench report.

- `--replication=async|quorum`: quorum mode means a batch is ACKed to clients only after majority fsync-ack. The honest latency cost gets measured in the bench report.

### R4.3 — The Raft decision (current Phase 16) needs an ADR *before* code
- `hashicorp/raft` brings its own log store and snapshot model — bolting it onto a single-writer engine means either abandoning your WAL (throwing away the project's centerpiece) or fighting the library. Recommended: strengthen the native protocol with fencing + quorum (R4.1/R4.2) and write ADR-005 "Why not hashicorp/raft." If consensus is still wanted after that, evaluate etcd's `linearizable` read path patterns or a minimal VR-style implementation — but that's a project of its own; don't let it eat the hardening phases.


### R4.4 — Backup/restore
- Snapshot + WAL archive to object storage; PITR tool; documented restore drill in the runbook (ties to Phase 19's runbook, P5.8).
- Snapshot + WAL archive to object storage; PITR tool; documented restore drill in the runbook (ties to Phase 19's runbook, P5.8).

---

## Tier 5 — Production hardening & engineering process

- **P5.1 CI (GitHub Actions):** build + vet + `golangci-lint` + `test -race` + coverage report + docker build + bench smoke (assert no >10% regression on a fixed workload). Hours of work, pays for itself immediately. Required to enforce T3.5.
- **P5.1 CI (GitHub Actions):** build + vet + `golangci-lint` + `test -race` + coverage report + docker build + bench smoke (assert no >10% regression on a fixed workload). Hours of work, pays for itself immediately. Required to enforce T3.5.
- **P5.2 Makefile + lint config:** `make proto lint test race bench cover docker`; import ordering, error-wrapping conventions.
- **P5.3 Security (current Phase 14):** TLS/mTLS for gRPC + replication listener (replication is currently plaintext TCP — it carries financial data), HMAC/client auth, token-bucket rate limiting per API key. The replication listener is more urgent than the public API.
- **P5.4 Lifecycle:** config validation at startup (fail fast on bad env), graceful drain (stop accepting, flush batch, final sync, stop replication), readiness = recovered + (if follower) caught-up; k8s probes wired to that. C0.5 readiness hook lives here.
- **P5.5 Deploy reality pass:** make docker-compose actually run primary+follower end-to-end (C0.9 ✅), fix follower WAL volume (C0.9 gap), fix Prometheus targets (C0.10.4), add Grafana dashboard JSON for the metrics that exist (transfer rate, batch size, sync latency, queue depth, lag).
- **P5.6 Observability debt:** set `RingBufferDepth` (C0.10.2) or delete; add follower-lag, segment count, recovery duration, snapshot age/size, idempotency evictions, invariant-violation counter (C0.7); alert rules for sync-latency p99 and invariant failure (a metric that should never fire but exists).
- **P5.4 Lifecycle:** config validation at startup (fail fast on bad env), graceful drain (stop accepting, flush batch, final sync, stop replication), readiness = recovered + (if follower) caught-up; k8s probes wired to that. C0.5 readiness hook lives here.
- **P5.5 Deploy reality pass:** make docker-compose actually run primary+follower end-to-end (C0.9 ✅), fix follower WAL volume (C0.9 gap), fix Prometheus targets (C0.10.4), add Grafana dashboard JSON for the metrics that exist (transfer rate, batch size, sync latency, queue depth, lag).
- **P5.6 Observability debt:** set `RingBufferDepth` (C0.10.2) or delete; add follower-lag, segment count, recovery duration, snapshot age/size, idempotency evictions, invariant-violation counter (C0.7); alert rules for sync-latency p99 and invariant failure (a metric that should never fire but exists).
- **P5.7 Format versioning:** version headers in WAL records, snapshot files, and replication frames; refuse-to-start (with clear error) on future versions; write the compatibility policy down. Cheapest now, impossible later.
- **P5.8 Release process:** tagged releases, changelog, `goreleaser` or docker image publishing from CI.
- **P5.9 Health & readiness endpoints (small but important):** `/healthz` currently returns `{"status":"UP"}` unconditionally. Split into `/livez` (process up) and `/readyz` (recovered + checkpointer started + caught-up-if-follower). k8s probes wire to these.
- **P5.9 Health & readiness endpoints (small but important):** `/healthz` currently returns `{"status":"UP"}` unconditionally. Split into `/livez` (process up) and `/readyz` (recovered + checkpointer started + caught-up-if-follower). k8s probes wire to these.

---

## Tier 6 — Narrative & portfolio signal (the actual point of the project)

1. **Rewrite the benchmark report honestly** (S2.4). Include the "what I measured, what broke, what I fixed, what remains structurally hard" arc — a principal engineer's value is demonstrated by the *analysis*, not the peak number.
2. **README claims audit:** every table row must be demonstrably true (e.g., "Lock-free state machine" is now true post-C0.1; add links from each claim to the test that proves it).
3. **New ADRs** for: read-path design (C0.1), batch atomicity choice (C0.4), batched API protocol (D1.1), transfer history index (D1.5), two-phase transfers (D1.2), replication protocol + why-not-raft (R4.3), snapshot+recovery contract (C0.5/C0.13). Supersede, don't edit, old ADRs.
4. **A killer README demo:** one command that brings up the compose stack, runs a batched load, kills the primary mid-flight, promotes the follower, and shows zero lost committed transfers with the auditor (T3.4) verifying the invariant. That single demo *is* the portfolio.
2. **README claims audit:** every table row must be demonstrably true (e.g., "Lock-free state machine" is now true post-C0.1; add links from each claim to the test that proves it).
3. **New ADRs** for: read-path design (C0.1), batch atomicity choice (C0.4), batched API protocol (D1.1), transfer history index (D1.5), two-phase transfers (D1.2), replication protocol + why-not-raft (R4.3), snapshot+recovery contract (C0.5/C0.13). Supersede, don't edit, old ADRs.
4. **A killer README demo:** one command that brings up the compose stack, runs a batched load, kills the primary mid-flight, promotes the follower, and shows zero lost committed transfers with the auditor (T3.4) verifying the invariant. That single demo *is* the portfolio.
5. **Comparison matrix** vs TigerBeetle (the table in §0, kept up to date) — framing gaps honestly reads as strength, not weakness.
6. **Kill the sleep-based tests:** `replication_test.go:67` and `failover_test.go:63` sleep 200ms and hope. Tier 3 makes them deterministic; use a `WaitForLSN(t, follower, n, 5*time.Second)` helper instead.
6. **Kill the sleep-based tests:** `replication_test.go:67` and `failover_test.go:63` sleep 200ms and hope. Tier 3 makes them deterministic; use a `WaitForLSN(t, follower, n, 5*time.Second)` helper instead.

---

## Suggested re-plan (supersedes Phases 14–19 in `Status.md`)

| New phase | Content | Tier |
|---|---|---|
| 14 | **Correctness & integrity hardening** — C0.5, C0.7, C0.8, C0.10, C0.11–C0.16, plus CI with `-race` (P5.1, P5.2, T3.5) | 0 |
| 15 | **Storage engine & honest benchmarks** — S2.1, S2.3, C0.14, batched API D1.1, C0.15, C0.16, report rewrite S2.4 | 1–2 |
| 14 | **Correctness & integrity hardening** — C0.5, C0.7, C0.8, C0.10, C0.11–C0.16, plus CI with `-race` (P5.1, P5.2, T3.5) | 0 |
| 15 | **Storage engine & honest benchmarks** — S2.1, S2.3, C0.14, batched API D1.1, C0.15, C0.16, report rewrite S2.4 | 1–2 |
| 16 | **Two-phase + linked transfers, rich schema** — D1.2–D1.4, D1.6 | 1 |
| 17 | **Query APIs & history index** — D1.5 | 1 |
| 18 | **Verification suite** — T3.1–T3.5 (auditor tool T3.4) | 3 |
| 19 | **Replication protocol, fencing, quorum mode** — R4.1–R4.3 (+ remaining C0.8 items if not done in 14) | 4 |
| 20 | **Security & ops** — P5.3–P5.9, chaos + runbook (old 14+19) | 5 |
| 18 | **Verification suite** — T3.1–T3.5 (auditor tool T3.4) | 3 |
| 19 | **Replication protocol, fencing, quorum mode** — R4.1–R4.3 (+ remaining C0.8 items if not done in 14) | 4 |
| 20 | **Security & ops** — P5.3–P5.9, chaos + runbook (old 14+19) | 5 |
| 21 | **Preallocation/direct-I/O + performance campaign** — S2.2, S2.4 final | 2 |
| 22 | **Narrative polish** — Tier 6 | 6 |

### Two-week shape (suggested)

The first two weeks should be a single, visible, defense-able slice:

- **Week 1, items 1–3:** C0.5 (wire checkpointer), C0.10 (housekeeping), C0.11/C0.12 (REST bug fixes), P5.1/P5.2 (CI + Makefile).
- **Week 1, items 4–6:** C0.7 (replace `Balance` panic), C0.13 (snapshot-only recovery), C0.14 (segment scan without full read).
- **Week 2, items 7–9:** C0.8.1 (follower batched `AppendBatch`), C0.8.2 (exponential backoff on bad CRC), C0.8.3 (term/epoch fencing).
- **Week 2, items 10–12:** S2.4 (honest bench rewrite at the current 2,900 TPS number with a forward-looking plan), D1.1 (batched API — start, do not finish), Tier 6.1 (annotate the README claims table).

By end of week 2: `go test -race ./...` in CI, `golangci-lint` clean, no `panic(` in hot-path packages, all four open Tier-0 items closed, bench report honest, REST 503/200/404/422 correct, follower has fencing. Then Tier 1/2 deep work begins.

---
### Two-week shape (suggested)

The first two weeks should be a single, visible, defense-able slice:

- **Week 1, items 1–3:** C0.5 (wire checkpointer), C0.10 (housekeeping), C0.11/C0.12 (REST bug fixes), P5.1/P5.2 (CI + Makefile).
- **Week 1, items 4–6:** C0.7 (replace `Balance` panic), C0.13 (snapshot-only recovery), C0.14 (segment scan without full read).
- **Week 2, items 7–9:** C0.8.1 (follower batched `AppendBatch`), C0.8.2 (exponential backoff on bad CRC), C0.8.3 (term/epoch fencing).
- **Week 2, items 10–12:** S2.4 (honest bench rewrite at the current 2,900 TPS number with a forward-looking plan), D1.1 (batched API — start, do not finish), Tier 6.1 (annotate the README claims table).

By end of week 2: `go test -race ./...` in CI, `golangci-lint` clean, no `panic(` in hot-path packages, all four open Tier-0 items closed, bench report honest, REST 503/200/404/422 correct, follower has fencing. Then Tier 1/2 deep work begins.

---

## What I would deliberately NOT do

- **Not** adopt hashicorp/raft as-is (R4.3) — it discards the project's centerpiece (your WAL + loop) for library convenience.
- **Not** chase io_uring immediately — it's Linux-only, high-effort, and irrelevant until D1.1 + S2.1 remove the software bottlenecks.
- **Not** add more breadth (more APIs, more clients, more languages) before depth — the repo already has more surface than substance in places (unwired subsystems).
- **Not** keep the Postgres baseline in the server path — it's the right benchmark comparator and nothing more.
- **Not** ship the `rpc-test` CLI command "for now" — every day it stays is a day a reviewer assumes it's intentional.

---

## Change log

- **2026-09-03:** initial roadmap.
- **2026-09-04:** rewritten to reflect post-commit state. Six Tier-0 items closed (C0.1, C0.2, C0.3, C0.4, C0.6, C0.9). Four still open (C0.5, C0.7, C0.8, C0.10). Six newly-surfaced items added: C0.11–C0.16 (REST error mapping, REST ID aliasing, double-recover, full-segment read on truncate, channel-allocation pressure, batcher unfairness). Tier 5 expanded with P5.9 (livez/readyz split). Execution plan extended with a two-week concrete shape.
