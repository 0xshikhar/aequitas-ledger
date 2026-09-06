# aequitas-ledger — Improvement Roadmap

**Date:** 2026-09-03
**Purpose:** Close the gap between "feature-complete portfolio project" and "system a principal engineer would sign their name to," measured against the stated inspiration (TigerBeetle).

**How to read this:** Tiers are priority order. Within Tier 0, every item is a *correctness* defect — nothing else in this document matters until those are fixed, because the project's core claim ("strict invariant survives crashes and replay") is currently falsifiable. Each item lists **Problem / Evidence / Fix / Proof** so work can be tracked and reviewed like the existing phases in `Status.md`.

---

## 0. Honest current-state audit

### What is genuinely strong

- The architectural skeleton is right: MPMC ring buffer → batcher → single-writer loop → group-commit WAL → deterministic apply is the correct shape and mirrors TigerBeetle's design philosophy.
- `Uint128` + fixed-size 64-byte accounts with compile-time size guards; value semantics in the hot path.
- CRC-checked WAL records with truncated-tail recovery; crash-recovery integration test exists.
- Real (not toy) supporting surface: gRPC + REST, replication, failover, CLI, docker-compose/k8s, ADRs, benchmark harness with a Postgres baseline.
- Test discipline is above average: fuzz targets, concurrent invariant tests, crash-recovery and failover integration tests.

### What undermines it today

1. **The core invariant is not actually guaranteed** — there are live correctness bugs (Tier 0) including a data race on the read path, non-durable accounts, torn-batch replay, and a follower that auto-creates zero-balance accounts.
2. **Performance is 2,900 TPS, not 200k–500k.** `docs/benchmark_report.md` measures ~2,900 TPS (0.34 ms/op concurrent) yet concludes "verifying all performance design goals." The README target is 200k–500k TPS. A reviewer who reads both will trust nothing else in the repo.
3. **Several subsystems are unwired decoration.** The snapshot checkpointer is never started; replication is library code the server binary never runs (`LEDGER_ROLE`/`PRIMARY_ADDR` are read by no Go code); the events publisher is dead code; a Prometheus gauge is never set; the follower k8s Deployment has no WAL volume; `deploy/prometheus.yml` scrapes a target that exists nowhere.
4. **No CI, no lint, no `-race` enforcement, no Makefile.** The read-path race would have been caught by one `go test -race` invocation in CI.
5. **Repo hygiene:** the entire `docs/` tree (ADRs, benchmark report, Status.md) is untracked in git while Status.md marks "ADRs complete and committed"; `cmd/ledger-cli` has an uncommitted, unrelated Ethereum JSON-RPC test command with hardcoded third-party endpoints that also dragged `golang.org/x/net/websocket` into `go.mod`.

### The TigerBeetle gap, summarized

| Dimension | TigerBeetle | aequitas today |
|---|---|---|
| API model | Batched: ~8K transfers per message, array of per-item results | Unary, one `chan error` per transfer |
| Transfer lifecycle | Two-phase: pending → post/void, timeout, linked chains | Immediate post only |
| Schema | ledger, code, user_data_128/64, flags semantics | id/currency/balances + frozen/closed only |
| Read path | Query API with filters + cursor pagination | Transfers are write-only (no read path at all); accounts readable via racy path |
| Storage | Preallocated static files, direct I/O, io_uring, own page cache, no full-WAL replay at startup | Buffered `WriteAt` per record, whole segments read into memory on recovery, full replay |
| Correctness testing | VOPR deterministic fault-injection simulator, run continuously in CI | Happy-path + single-point crash tests |
| Replication | Quorum-replicated state machine, epochs, fencing | Best-effort async stream, full WAL rescan per reconnect, no acks/quorum/fencing |
| Throughput | ~1M TPS class | ~2,900 TPS |

---

## Tier 0 — Correctness debt (do this before anything else)

> **Guiding rule:** a ledger earns trust by what it refuses to get wrong. Every item here is observable by a reviewer and currently wrong.

### C0.1 — Read-path data race
- **Problem:** API goroutines read/write `AccountManager` while the event loop mutates the same slice/map. `go test -race` on a mixed read/write workload will report races; torn `Uint128` balance reads are possible.
- **Evidence:** `internal/engine/ledger.go:134-165` (`CreateAccount`/`GetAccount`/`GetBalance` call `accounts.Create/Get` directly); `internal/engine/accounts.go:5-35` (no synchronization, `Get` returns a pointer into the live slice); the loop concurrently runs `ApplyBatch` (`loop.go:93`). The `snapshotRequests` channel (`loop.go:50-52`) exists exactly for this and is only used by the (unwired) checkpointer.
- **Fix:** Route all reads and account creation through the event loop (request/response channel like `RequestSnapshotView`), or move to a seqlock/epoch-based reader scheme if read latency through the loop is unacceptable. Account creation must become an event type in the ring buffer — which also fixes C0.2.
- **Proof:** Integration test with concurrent `GetAccount` while transfers are in flight, run under `-race` in CI (must be clean). Document read-path design in an ADR (seqlock vs loop-hop).

### C0.2 — Accounts are not durable
- **Problem:** `CreateAccount` never writes a WAL record. Accounts created via the API vanish on restart; the conservation invariant is silently destroyed after recovery (transfers referencing them replay into errors).
- **Evidence:** `RecordTypeAccount` is defined (`internal/wal/record.go`) but written by nothing; `Ledger.Recover()` only replays transfers (`ledger.go:167-181`); all tests seed accounts via `Config.InitialAccounts`, which masks the bug.
- **Fix:** Account create/close/freeze go through the event loop as first-class WAL-recorded operations (see C0.1). Recovery replays both record types.
- **Proof:** Integration test: create account via API → transfer → kill -9 → restart → account, balances, and invariant all present. No `InitialAccounts` seeding allowed.

### C0.3 — LSN accounting is broken
- **Problem:** Live LSN increments once per batch; recovery counts per record. A process that wrote 10 batches × 20 transfers has live LSN 10 and recovers to LSN 200. LSN is also not stored in records, so it cannot be used for replication or truncation correctness.
- **Evidence:** `internal/wal/wal.go:89` (`currentLSN++` per `AppendBatch` call) vs `internal/wal/recovery.go` (`recoveredLSN += recs`); `TruncateBefore(lsn)` ignores its argument entirely and deletes every non-current segment (`wal.go:125-139`).
- **Fix:** Define LSN as monotonically increasing **per record** (or per batch-commit, but pick one); write it into the record header; make `TruncateBefore` segment-aware (only delete segments whose max LSN < snapshot LSN).
- **Proof:** Unit test: N batches of M records → restart → `CurrentLSN()` identical pre/post. `TruncateBefore(lsn)` deletes exactly the segments below that LSN and recovery still succeeds.

### C0.4 — WAL batches are not atomic (torn-batch replay)
- **Problem:** A crash mid-`Sync` can persist a prefix of a batch. Replay accepts the prefix, so a "committed" batch can be partially applied — the exact failure mode group commit exists to prevent.
- **Evidence:** Record format is `[type|len|payload|crc32]` per record with no batch framing or commit marker (`internal/wal/record.go`); recovery replays whatever valid records it finds.
- **Fix (choose one, write an ADR):** (a) batch-commit record: payload records first, then a `BatchCommit(batch_id, count, checksum)` record — replay stops at the last committed batch; or (b) whole-batch checksum in a batch header. TigerBeetle-style: prepare/commit within the segment, checksums over the full batch.
- **Proof:** Crash test that truncates the WAL at every byte offset of a multi-record batch; recovery must yield either all-or-nothing of that batch, and the invariant must hold in every case (this doubles as T3.1 seed work).

### C0.5 — Snapshot subsystem is unwired *and* unsafe to wire
- **Problem:** The checkpointer is never started; snapshots are never read at startup. If someone wires it as-is, `TruncateBefore` deletes WAL segments while recovery still depends on them — data loss.
- **Evidence:** No references to `internal/snapshot` outside its own package and `tests/unit/snapshot_test.go`; `TruncateBefore` ignores its LSN (C0.3).
- **Fix:** Wire the full path in dependency order: startup loads latest snapshot (format-versioned header, see P5.7) → replays WAL from snapshot LSN → checkpointer runs on interval → `TruncateBefore(snapshotLSN)` only after a successful snapshot load has been proven by a test. Requires C0.3 fixed first.
- **Proof:** Test: write 100k transfers → snapshot → truncate → restart → balances identical to pre-restart truth, and recovery time measurably faster than full replay (report both numbers).

### C0.6 — Idempotency is not durable
- **Problem:** The idempotency store is memory-only. After a crash+restart, a client retrying a timed-out transfer with the same key gets a **double debit**. For a payments ledger this is the most expensive failure mode there is.
- **Evidence:** `internal/engine/idempotency.go` (no persistence); `Recover()` does not restore keys. The WAL already contains the idempotency key inside every transfer record (`engine/codec.go`) — the data needed is there.
- **Fix:** During recovery, rebuild the idempotency index from replayed transfer records (respecting TTL semantics — store transfer timestamp, not insertion time). Optionally cap rebuild to the TTL window.
- **Proof:** Test: transfer with key K → kill → restart → retry K → returns the original result, balances unchanged.

### C0.7 — Panics inside the state machine
- **Problem:** `core.Balance()` and `ApplyDebit` panic on underflow/missing account. A panic in the single-writer goroutine takes down the whole server; in a library it's a caller-crash bug.
- **Evidence:** `internal/core/account.go` (`Balance` panics on underflow), `internal/engine/accounts.go:78-89` (`ApplyDebit` panics on missing account and overflow).
- **Fix:** Return errors; assert at batch boundaries instead. Keep `assertInvariant` as an explicit, tested guard that fails loudly (metric + shutdown) rather than a stray panic.
- **Proof:** Unit tests for underflow/overflow/missing-account paths returning typed errors; grep-gate for `panic(` in `internal/engine` + `internal/core` in CI.

### C0.8 — Replication apply is unsound
- **Problem:** The follower applies streamed transfers **without validation**, auto-creating unknown accounts with zero balance (silently destroying the conservation invariant on the follower), swallows decode errors, and the wire frame has no checksum.
- **Evidence:** `internal/replication/follower.go` (`_ = f.accounts.Create(...)` on unknown account; `if err == nil` swallow), `server.go` frame `[LSN|type|len|payload]` with no CRC.
- **Fix:** Follower replays through the same validation/apply code path as recovery (with a "trusted replay" mode that validates structural invariants); CRC every wire frame; on any inconsistency, drop state and re-sync from scratch — never paper over.
- **Proof:** Test that a corrupted frame and an unknown-account transfer both cause a controlled re-sync, not silent divergence; follower-side invariant assertion after every catch-up.

### C0.9 — The server cannot actually run as a follower
- **Problem:** docker-compose/k8s set `LEDGER_ROLE` and `PRIMARY_ADDR`, but no Go code reads them. The primary never starts `replication.Server`; there is no follower mode in `cmd/server`. The HA story is library code exercised only by tests.
- **Evidence:** Grep for `LEDGER_ROLE`/`PRIMARY_ADDR` in Go: zero hits. `cmd/server/main.go` wires neither.
- **Fix:** Real role lifecycle in `cmd/server`: primary starts the replication listener; follower connects, persists to its own WAL (currently it doesn't — promotion state comes only from memory), exposes read-only APIs, and refuses writes. Follower readiness = caught up within N.
- **Proof:** docker-compose-based test (or integration test): bring up primary + follower, write, verify follower serves reads and rejects writes, promote, verify writes flow to a new follower. Also fix: follower k8s Deployment gets a WAL volume.

### C0.10 — Housekeeping / dead code / hygiene
- **Problem:** Dead and unrelated code erodes reviewer trust faster than missing features.
- **Items:**
  - Remove or extract the uncommitted Ethereum `rpc-test` command from `cmd/ledger-cli/main.go` (it does not belong in a ledger CLI; it also added `golang.org/x/net/websocket` to `go.mod`).
  - Delete dead code: `wal.encodeBatch`, `replication.dummyWriter`/`encodeTransfer`, `AccountCreateEvent` (or use it for C0.2 — preferred), `core.ErrDuplicateTransferID` (or enforce it — preferred), `events.Publisher` (or wire the outbox to it).
  - Set or remove the never-updated `observability.RingBufferDepth` gauge.
  - Commit the `docs/` tree; Status.md claims ADRs are "committed" but `docs/` is untracked.
  - Fix `deploy/prometheus.yml` scraping a target that matches no service.
- **Proof:** `golangci-lint` (unused/deadcode) clean; `git status` clean after the phase.

---

## Tier 1 — TigerBeetle domain depth (the "inspiration" delta)

These are what make TigerBeetle a *financial* database rather than a fast KV with balances.

### D1.1 — Batched request/response protocol *(single biggest TPS lever)*
- Replace unary `CreateTransfer` with `CreateTransfers([]Transfer) -> []Result` (batch up to ~1K–8K items per RPC), same for accounts. This eliminates the per-transfer `chan error` allocation, amortizes API overhead across the batch, and is how TigerBeetle gets its throughput. Internals: one submit + one result slice + single completion notification per batch instead of N channels.
- REST equivalent: accept arrays. Results array mirrors TigerBeetle semantics: per-item status, batch accepted atomically at the protocol level.
- **Proof:** bench: batch-8192 RPC ≥ 25× unary TPS; allocs/op on submit path near zero.

### D1.2 — Two-phase transfers (pending → post/void)
- Add `PendingDebits/PendingCredits` to accounts (the proto already reserves these fields), `timeout` on transfers, and `post_pending_transfer` / `void_pending_transfer` flags. This models holds/authorizations — the reason payment systems look the way they do — and multiplies validation logic in interesting, testable ways.
- **Proof:** Unit tests for hold-expiry (logical clock), post/void idempotency, insufficient-available-funds vs posted balance.

### D1.3 — Linked transfers (chains)
- `flag_linked` semantics: a batch containing linked transfers commits atomically or fails as a chain (two-phase across accounts — escrow patterns: debit A/credit escrow + escrow/debit B/credit B in one atomic chain).
- **Proof:** Chain-failure test: link of 3 where #2 fails → nothing applied.

### D1.4 — Rich schema: ledger, code, user_data
- `ledger u32` (multi-tenant isolation; invariant becomes per-ledger), `code u16` (transfer type / account type), `user_data_128`/`user_data_64` (opaque correlation fields the database promises to store and return, never interpret). These are cheap to add now, impossible to add later without a migration story.
- **Proof:** Invariant checker scope becomes per-ledger; API round-trips user_data byte-for-byte.

### D1.5 — Query APIs (there is currently NO transfer read path)
- `GetTransfers` with filters (account_id, code, ledger, time range, flags) + cursor pagination. Requires a transfer history index — design decision for an ADR: per-account append-only log of transfer IDs + WAL-offset index vs periodic sorted on-disk segments.
- `GetAccountTransfers`, `GetAccountBalances` (time-travel is a stretch goal).
- **Proof:** Integration test paginates a 100k-transfer history with a cursor; no unbounded memory use.

### D1.6 — Timestamp discipline
- Batch-synchronous timestamps (one per batch, exposed to clients) rather than `time.Now()` inside the loop; recovery must preserve original timestamps (it does today — keep it that way and test it).

---

## Tier 2 — Storage engine & performance (earn the 200k TPS claim or retract it)

Current cost structure per transfer: unary RPC + per-transfer channel alloc + `WriteAt` per record + small effective batches + busy-spin loops. Staged plan:

### S2.1 — Make the write path cheap before making it clever
- Encode the whole batch into one buffer; one sequential `Write` (append), not per-record `WriteAt`.
- `fdatasync` on Linux (cheaper than `fsync`; macOS fallback).
- Right-size batching: with unary RPCs the batcher rarely sees >a few events; fix D1.1 first or the rest is noise.
- Remove busy-spin: block the loop on a notify channel + timer for `BatchTimeout` instead of `default:` + `Gosched` spin (`loop.go:45-61`, `batch.go`). Idle CPU should be ~0.
- **Proof:** pprof flame graphs before/after committed to the bench report; idle CPU < 1%.

### S2.2 — File preallocation + direct I/O path
- `fallocate` segments at creation (no filesystem metadata churn mid-write, no fragmentation); Linux `O_DIRECT` with aligned buffers and a write-through path; keep buffered fallback for macOS/dev. TigerBeetle's core bet is that a database should own its I/O — this is the Go-pragmatic version.
- **Proof:** Bench on Linux showing p99 sync-latency stability under sustained load (this is the honest motivation: tail latency, not mean TPS).

### S2.3 — Recovery that doesn't read whole segments into RAM
- Stream records with a bounded buffer instead of `make([]byte, fileSize)`; combined with C0.5 snapshots, startup becomes O(WAL since snapshot) not O(all history).
- **Proof:** Restart with 10GB WAL: RSS stays bounded; recovery-time metric reported.

### S2.4 — The honest performance campaign (rewritten `benchmark_report.md`)
- Staged targets with evidence at each stage: 10k → 50k → 100k+ TPS on your hardware, each with a profile identifying the then-current bottleneck. If a stage stalls, the report says why and what the structural limit is — *that* is principal-level content. Include: TPS at batch sizes 1/128/1024/8192, p50/p95/p99/fsync distribution, hot-account contention scenario, and recovery time vs WAL size.
- Retract or requalify the 200k–500k README claim until measured. Nothing in the repo is more damaging than a claim the numbers contradict.

---

## Tier 3 — Verification: test like a database, not a web app

This is TigerBeetle's actual crown jewel (VOPR). A Go-scale version is very achievable and is the highest credibility-per-effort work available.

### T3.1 — Crash-at-every-LSN property test
- For a seeded workload: for each LSN (or each byte offset — stronger), truncate the WAL there, recover, assert invariants and expected balances (recomputed by an independent model). Run as a normal `go test` with a bounded corpus + a long-run mode for CI nightly.
### T3.2 — Fault-injecting WAL wrapper
- A `filesystem` interface in front of the WAL: inject bit flips after write, torn writes, short reads, `ENOSPC`, reordered barriers pre-fsync. Every injected fault must end in either a clean recovery or a loud, typed failure — never a silent divergence.
### T3.3 — Replication adversarial tests
- Partition/lag/corrupt/duplicate frames between primary and follower (in-process pipe with a chaotic middle). Follower must re-sync or refuse — never diverge. Kill follower mid-sync, promote, verify no lost committed transfers (defines "committed" = fsynced, and later = quorum-replicated).
### T3.4 — First-class invariant auditor
- `ledger-cli audit --wal <dir>`: independent replayer (not the engine's own recovery code) that recomputes balances from raw WAL bytes and verifies conservation + batch atomicity + idempotency-key uniqueness. Shipping the auditor is a strong signal: the system grades itself with code that doesn't share the engine's assumptions.
### T3.5 — CI enforcement
- `go test -race ./...` on every PR (would have caught C0.1 immediately), fuzz targets with time budget in CI, coverage gate on `internal/wal` + `internal/engine` (e.g., 85%+), nightly long-run property tests.

---

## Tier 4 — Replication & distribution correctness

### R4.1 — A real replication protocol (before any Raft)
- Live tailing (follower stays connected, primary streams new commits) instead of full-WAL-rescan-per-reconnect.
- Follower ACKs → primary tracks per-follower commit index; expose replication lag as a metric and readiness condition.
- Epoch/term fencing on promotion: a promoted follower carries a term; stale primaries cannot corrupt (write fencing in the protocol, `ErrNotLeader` with redirect hint on writes).
- Follower persists its own WAL (C0.9) so promotion doesn't depend on memory.
### R4.2 — Configurable durability
- `--replication=async|quorum`: quorum mode means a batch is ACKed to clients only after majority fsync-ack (this is the RWMajority idea, and the honest latency cost gets measured in the bench report).
### R4.3 — The Raft decision (current Phase 16) needs an ADR *before* code
- `hashicorp/raft` brings its own log store and snapshot model — bolting it onto a single-writer engine means either abandoning your WAL (throwing away the project's centerpiece) or fighting the library. Recommended: strengthen the native protocol with fencing + quorum (R4.1/R4.2) and write ADR-005 "Why not hashicorp/raft." If consensus is still wanted after that, evaluate etcd's `linearizable` read path patterns or a minimal VR-style implementation — but that's a project of its own; don't let it eat the hardening phases.
### R4.4 — Backup/restore
- Snapshot + WAL archive to object storage; PITR tool; documented restore drill in the runbook (Ties to Phase 19's runbook).

---

## Tier 5 — Production hardening & engineering process

- **P5.1 CI (GitHub Actions):** build + vet + `golangci-lint` + `test -race` + coverage report + docker build + bench smoke (assert no >10% regression on a fixed workload). This is hours of work and pays for itself immediately.
- **P5.2 Makefile + lint config:** `make proto lint test race bench cover docker`; import ordering, error-wrapping conventions.
- **P5.3 Security (current Phase 14):** TLS/mTLS for gRPC + replication listener (replication is currently plaintext TCP — it carries financial data), HMAC/client auth, token-bucket rate limiting per API key. The replication listener is more urgent than the public API.
- **P5.4 Lifecycle:** config validation at startup (fail fast on bad env), graceful drain (stop accepting, flush batch, final sync, stop replication), readiness = recovered + (if follower) caught-up; k8s probes wired to that.
- **P5.5 Deploy reality pass:** make docker-compose actually run primary+follower end-to-end (C0.9), fix follower WAL volume, fix Prometheus targets, add Grafana dashboard JSON for the metrics that exist (transfer rate, batch size, sync latency, queue depth, lag).
- **P5.6 Observability debt:** set `RingBufferDepth`; add follower-lag, segment count, recovery duration, snapshot age/size, idempotency evictions; alert rules for sync-latency p99 and invariant failure (a metric that should never fire but exists).
- **P5.7 Format versioning:** version headers in WAL records, snapshot files, and replication frames; refuse-to-start (with clear error) on future versions; write the compatibility policy down. Cheapest now, impossible later.
- **P5.8 Release process:** tagged releases, changelog, `goreleaser` or docker image publishing from CI.

---

## Tier 6 — Narrative & portfolio signal (the actual point of the project)

1. **Rewrite the benchmark report honestly** (S2.4). Include the "what I measured, what broke, what I fixed, what remains structurally hard" arc — a principal engineer's value is demonstrated by the *analysis*, not the peak number.
2. **README claims audit:** every table row must be demonstrably true (e.g., "Lock-free state machine" is false until C0.1 lands; add links from each claim to the test that proves it).
3. **New ADRs** for: read-path design, batch atomicity (C0.4 choice), batched API protocol, transfer history index (D1.5), two-phase transfers, replication protocol + why-not-raft (R4.3). Supersede, don't edit, old ADRs.
4. **A killer README demo:** one command that brings up the compose stack, runs a batched load, kills the primary mid-flight, promotes the follower, and shows zero lost committed transfers with the auditor verifying the invariant. That single demo *is* the portfolio.
5. **Comparison matrix** vs TigerBeetle (the table in §0, kept up to date) — framing gaps honestly reads as strength, not weakness.
6. **Kill the sleep-based tests:** replication/failover tests sleep 200ms and hope (Tier 3 makes them deterministic).

---

## Suggested re-plan (supersedes Phases 14–19 in `Status.md`)

| New phase | Content | Tier |
|---|---|---|
| 14 | **Correctness & integrity hardening** — C0.1–C0.10, plus CI with `-race` (P5.1) | 0 |
| 15 | **Storage engine & honest benchmarks** — S2.1, S2.3, batched API D1.1, report rewrite | 1–2 |
| 16 | **Two-phase + linked transfers, rich schema** — D1.2–D1.4, D1.6 | 1 |
| 17 | **Query APIs & history index** — D1.5 | 1 |
| 18 | **Verification suite** — T3.1–T3.5 (auditor tool) | 3 |
| 19 | **Replication protocol, fencing, quorum mode** — R4.1–R4.3 (+ C0.8/C0.9 if not done in 14) | 4 |
| 20 | **Security & ops** — P5.3–P5.8, chaos + runbook (old 14+19) | 5 |
| 21 | **Preallocation/direct-I/O + performance campaign** — S2.2, S2.4 final | 2 |
| 22 | **Narrative polish** — Tier 6 | 6 |

Rationale for the reordering vs the current plan: old Phase 14 (rate limiting/TLS) and Phase 16 (Raft) both build on a foundation that currently has data-loss and race bugs; old Phase 17 (load generator) is instrumentation for a performance story that doesn't exist yet (S2.4 subsumes it); old Phase 18 (OTel) is nice-to-have after the metrics that matter exist (P5.6).

## What I would deliberately NOT do

- **Not** adopt hashicorp/raft as-is (R4.3) — it discards the project's centerpiece (your WAL + loop) for library convenience.
- **Not** chase io_uring immediately — it's Linux-only, high-effort, and irrelevant until D1.1 + S2.1 remove the software bottlenecks.
- **Not** add more breadth (more APIs, more clients, more languages) before depth — the repo already has more surface than substance in places (unwired subsystems).
- **Not** keep the Postgres baseline in the server path — it's the right benchmark comparator and nothing more.
