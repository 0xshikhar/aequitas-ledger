# 07 — Testing Like a Database

**Files in focus:** `tests/unit/`, `tests/integration/`, and the bug stories
that each test style would have caught.

A web app's tests ask "does the happy path work?" A database's tests ask
"what can the universe do to me, and do I stay correct?" This document
walks the testing techniques this repo uses, from table tests to
allocation-bound assertions, with the rule for when each is worth writing.

---

## 1. Layout: external test packages, two tiers

```
tests/
├── unit/          # package unit   — engine/wal/snapshot behavior, no network
└── integration/   # package integration — full ledger, gRPC bufconn, REST httptest, crash cycles
```

Tests live *outside* `internal/` in external test packages
(`package unit`, importing `aequitas-ledger/internal/...`). That is a
deliberate constraint: external packages can only use exported APIs, so the
test suite doubles as a **public API design review**. Anything hard to test
from outside is usually wrong to expose or wrongly shaped. The trade-off
(white-box tests can't reach internals) is covered by making the internals
testable through their public surface — e.g. `Ledger.CurrentLSN()` exists
precisely so the snapshot-authority test can observe the watermark.

Helpers live in `helpers_test.go` per package (`id16`, `mkID`,
`setupGRPCServer`) and take `testing.TB` where they serve both tests and
benchmarks — the interface `*testing.T` and `*testing.B` both satisfy:

```go
// tests/unit/wal_recover_lsn_test.go
func buildMultiSegmentWAL(t testing.TB, nBatches, batchSize, segmentSize int) (string, int64) {
```

## 2. Table-driven tests and subtests

```go
for _, tc := range []struct {
    name     string
    fromLSN  int64
    wantSeen int
}{
    {"resume at head replays nothing", head, 0},
    {"resume at zero replays everything", 0, nBatches * batchSize},
    {"resume mid-history replays only the delta", 13, 19},
} {
    t.Run(tc.name, func(t *testing.T) { ... })
}
```

The pattern: a slice of anonymous structs, `t.Run` per case (so `-run
TestName/case_name` isolates one), `t.Fatalf`/`t.Errorf` with got-vs-want in
the message. Add a case by adding a struct literal — no copy-pasted test
bodies. Use `t.Fatalf` to stop the test on unrecoverable setup failure,
`t.Errorf` to keep collecting failures.

## 3. The race detector is a test, not a tool

Every CI run must be `go test -race ./...`. The detector found this repo's
read-path race (C0.1) and its absence let it live for weeks. Rules that make
the detector actually work:

- **Concurrency tests must actually overlap.** A test that starts a goroutine
  and immediately waits proves nothing. `TestConcurrentReadWriteRace` runs
  writers, readers, and a creator for *seconds* under load.
- **Assertions after `cancel()` are not concurrency tests** — the original
  engine-loop test checked the invariant only after shutdown, when the race
  was long over. Overlap the assertion with the load.
- The detector sees only executed interleavings: more load, more chances.
  Sleep-based "waiting for catch-up" tests are both flaky and weak — replace
  them with condition polling (the roadmap kills the remaining 200 ms sleeps).

## 4. Fuzzing: the decoder's enemy is random bytes

```go
// tests/unit/record_test.go
func FuzzDecodeRecord_NoPanicOnRandomBytes(f *testing.F) {
    f.Add([]byte{...})     // seed corpus
    f.Fuzz(func(t *testing.T, data []byte) {
        _, _ = wal.DecodeRecord(data)   // must not panic; error is fine
    })
}
```

Run locally: `go test -fuzz FuzzDecodeRecord -fuzztime 30s ./tests/unit`.
The fuzzer mutates seeds through coverage guidance — it will find the
panic-on-malformed-length bug that hand-written cases miss. The contract
being fuzzed is precise: *decode never panics; it returns a record or an
error.* Every format decoder in the repo deserves one of these.

## 5. Property tests: sweeping the input space

The crash-recovery test family shows the difference between scenario tests
and property tests:

- **Scenario:** "kill after 100 transfers, restart, balances correct" — one
  point in a huge space. It passed for weeks while C0.17 lurked.
- **Property:** "for *every* truncation offset, recovery yields committed
  batches all-or-nothing and the invariant holds" — the whole space.
  `TestRecoverBatchStraddlingSegmentRotation` is a step in that direction;
  the roadmap's T3.1 (truncate at every byte offset, assert against an
  independent model) is the destination.

The strongest property tests use an **independent model**: recompute expected
balances with simple, obviously-correct code (a map, an append) and compare
against the engine. When the engine and the model disagree, one of them is
wrong and you get a reproducible input. This is also the idea behind the
planned `ledger-cli audit` tool: grade the engine with code that doesn't
share its assumptions.

## 6. Unusual but decisive: allocation-bound assertions

Some properties are about *resource behavior*, not outputs. Two tests assert
"we do not read bytes we shouldn't" via heap accounting:

```go
// tests/unit/wal_recover_lsn_test.go
var before, after runtime.MemStats
runtime.GC()
runtime.ReadMemStats(&before)
countTransfers(t, w, head)          // resume at head: skip everything
runtime.ReadMemStats(&after)
alloc := after.TotalAlloc - before.TotalAlloc
if alloc > limit { t.Fatalf(...) }  // reading 12 MiB of segments would blow this
```

`TotalAlloc` is a monotone counter of all bytes ever allocated — GC noise
doesn't matter, only the delta. When the property is "O(tail) not O(segment),
" a wall-clock test is flaky on shared CI machines; an allocation bound is
deterministic. Use it whenever the claim is about *how much work* something
does, not just its result.

## 7. Latency tests and budgets

```go
// tests/integration/read_latency_test.go
sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
p50, p99 := latencies[n/2], latencies[n*99/100]
if p50 > 2*time.Millisecond { t.Fatalf(...) }
if p99 > 10*time.Millisecond { t.Fatalf(...) }
```

Latency assertions need a *budget with a rationale*, or they flake. This
test's budget is documented in its doc comment: p99 is bounded by one batch
cycle (batch wait + fsync, which a fair non-preemptive loop cannot
interrupt) — hence 10 ms, not 1 ms. Two more lessons from this test: the
load must reproduce the failure regime (16 continuous writers, because 2
synchronous writers never starved the old code), and the sample must be big
enough for percentiles to mean something (≥500 reads).

## 8. Crash testing: kill -9 as a first-class test step

```go
// tests/integration/crash_recovery_test.go, snapshot_authority_test.go
l1.Close()                       // or os.Kill a subprocess in the real deal
// delete WAL segments entirely...
w2, _ := wal.Open(walDir, 8<<10) // reopen over the wreckage
l2, _ := engine.NewLedger(cfg, w2)
// assert balances + watermark against independently computed truth
```

In-process close-then-reopen over the same directory is the workhorse; a
subprocess killed with SIGKILL is the gold standard (real torn page-cache
state). The key discipline: **never seed the test with `InitialAccounts`
after the crash** — that was exactly how the non-durable-accounts bug (C0.2)
hid, because the reseeded accounts masked the missing replay. Test against
accounts created *through the API* before the crash.

> **Exercise 1.** Write `TestRecoverTruncateEveryOffset`: write one 5-record
> batch, then for every byte offset o in the segment, truncate to o, recover,
> and assert the batch is either fully present or fully absent. Compare with
> a model. (Hint: copy the WAL dir per offset — `os.CopyFS`.)
>
> **Exercise 2.** Add a fuzz target for `DecodeAccountPayload`. Run 60
> seconds. Any panic is a real bug in the format's trust boundary.
>
> **Exercise 3.** Find the two replication tests that still sleep 200 ms
> for catch-up and convert them to poll `follower.LastLSN()` with a
> deadline. Feel the flakiness disappear.

## Recap

| Technique | Catches | Example in repo |
|---|---|---|
| Table tests | behavior across cases | `TestRecoverFromLSNDeltaReplayOnly` |
| `-race` + overlapping load | memory races | `race_test.go` |
| Fuzzing | panics on malformed input | `FuzzDecodeRecord_NoPanicOnRandomBytes` |
| Property tests | state-machine scope bugs | straddle-batch test; T3.1 plan |
| Model-based checks | engine+test share the bug | planned auditor / T3.1 model |
| Allocation bounds | resource regressions | `TestTruncateBeforeDoesNotReadWholeSegments` |
| Latency budgets | starvation, queueing | `TestReadLatencyBoundedUnderWriteLoad` |
| Crash-restart cycles | durability holes | `crash_recovery_test.go`, snapshot authority |
