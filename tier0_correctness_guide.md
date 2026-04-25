# Tier 0 Correctness Issues — Complete Educational & Solution Guide

Welcome to the **Tier 0 Correctness Guide** for `aequitas-ledger`. 

In a financial system, **correctness is non-negotiable**. A database or ledger earns trust not by its peak TPS, but by what it **refuses to get wrong**.

This document breaks down every single Tier 0 issue (`C0.1` to `C0.10`) across 6 progressive dimensions:
1. **The Fundamental Concept** (Noob / Beginner level)
2. **Current Code & Root Cause Analysis** (Detailed code walkthrough)
3. **Catastrophic Production Failure** (Real-world consequences)
4. **Engineering Mental Model & Solution Design** (How a principal engineer thinks)
5. **Exact Code Fix (Before vs. After)** (Production-grade Go implementation)
6. **Automated Proof & Verification** (How to write tests that catch regressions forever)

---

# Table of Contents
- [C0.1 — Read-Path Data Race](#c01--read-path-data-race)
- [C0.2 — Non-Durable Accounts](#c02--non-durable-accounts)
- [C0.3 — Broken LSN (Log Sequence Number) Accounting](#c03--broken-lsn-log-sequence-number-accounting)
- [C0.4 — Torn-Batch Replay (Lack of Batch Atomicity)](#c04--torn-batch-replay-lack-of-batch-atomicity)
- [C0.5 — Snapshot Subsystem Unwired & Unsafe](#c05--snapshot-subsystem-unwired--unsafe)
- [C0.6 — Non-Durable Idempotency Store](#c06--non-durable-idempotency-store)
- [C0.7 — Panics Inside the State Machine](#c07--panics-inside-the-state-machine)
- [C0.8 — Unsound Replication Apply](#c08--unsound-replication-apply)
- [C0.9 — Server Unable to Run as Follower](#c09--server-unable-to-run-as-follower)
- [C0.10 — Housekeeping, Dead Code & Hygiene](#c010--housekeeping-dead-code--hygiene)

---

## C0.1 — Read-Path Data Race

### 1. The Fundamental Concept
In Go, **goroutines** are lightweight threads running concurrently. When two or more goroutines access the exact same memory location at the same time, and **at least one access is a write**, you have a **Data Race** unless you use synchronization (like a Mutex or Channel).

Why is this dangerous? Modern CPUs do not read memory byte-by-byte; they read in 64-byte **cache lines** and register words (64 bits). Our money type, `Uint128`, consists of two 64-bit integers:
```go
type Uint128 struct {
    Lo uint64
    Hi uint64
}
```
Updating `Uint128` requires **two CPU write instructions** (one for `Lo`, one for `Hi`). If a reader reads `Uint128` precisely between those two CPU instructions, it suffers a **torn read**: it reads the *new* `Lo` combined with the *old* `Hi`! The resulting balance is completely corrupted.

### 2. Current Code & Root Cause Analysis
Look at [`internal/engine/ledger.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/engine/ledger.go#L146-L157):

```go
// RUNNING IN API GOROUTINE (gRPC / HTTP handler)
func (l *Ledger) GetAccount(ctx context.Context, id [16]byte) (core.Account, error) {
    acc, err := l.accounts.Get(id) // <--- DIRECT UNSYNCHRONIZED READ!
    if err != nil {
        return core.Account{}, err
    }
    return *acc, nil
}
```
Meanwhile, the single-writer event loop in [`internal/engine/loop.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/engine/loop.go) is concurrently executing `ApplyBatch`, which mutates the same `Account` structs inside `l.accounts`!

There is **zero synchronization** between the API goroutines reading accounts and the single-writer goroutine updating balances.

### 3. Catastrophic Production Failure
A client requests their account balance (`GET /v1/accounts/123`). At the exact same microsecond, a $1,000,000 transfer is processed on their account. The reader thread captures a torn `Uint128` read, returning a balance of `$18,446,744,073,709,551,615` (2^64 - 1). The client sees invalid balance data, or `go test -race` crashes the service in staging.

### 4. Engineering Mental Model & Solution Design
How do we fix this while preserving our core architecture?
- **Option A (Hop through the Event Loop):** Route read requests through the single-writer goroutine via a request/response channel. Since the single-writer thread processes requests sequentially, reads and writes can never happen concurrently!
- **Option B (Copy-on-Write / RCU Read Map):** Maintain a read-only atomic pointer map that gets updated atomically whenever a batch finishes.

For our single-writer architecture, **Option A** is the cleanest, purest design. It guarantees 100% linearizability without adding mutexes to `AccountManager`.

### 5. Exact Code Fix (Before vs. After)

#### BEFORE ([ledger.go](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/engine/ledger.go#L146-L157)):
```go
func (l *Ledger) GetAccount(ctx context.Context, id [16]byte) (core.Account, error) {
    acc, err := l.accounts.Get(id) // Data Race!
    if err != nil {
        return core.Account{}, err
    }
    return *acc, nil
}
```

#### AFTER:
1. Define a `ReadAccountEvent` in `internal/engine/event.go`:
```go
type ReadAccountEvent struct {
    ID     [16]byte
    Result chan ReadAccountResult
}

type ReadAccountResult struct {
    Account core.Account
    Err     error
}
```

2. Add a channel in `Ledger` and handle it in `loop.go`:
```go
// In ledger.go:
func (l *Ledger) GetAccount(ctx context.Context, id [16]byte) (core.Account, error) {
    resChan := make(chan ReadAccountResult, 1)
    ev := ReadAccountEvent{ID: id, Result: resChan}
    
    select {
    case l.readQueue <- ev:
    case <-ctx.Done():
        return core.Account{}, ctx.Err()
    }

    select {
    case res := <-resChan:
        return res.Account, res.Err
    case <-ctx.Done():
        return core.Account{}, ctx.Err()
    }
}
```

### 6. Automated Proof & Verification
Write a concurrent read/write integration test in `tests/integration/race_test.go`:
```go
func TestConcurrentReadWriteRace(t *testing.T) {
    l := setupTestLedger(t)
    
    go func() {
        for i := 0; i < 1000; i++ {
            l.SubmitTransfer(...) // Write
        }
    }()
    
    for i := 0; i < 1000; i++ {
        _, _ = l.GetAccount(...) // Read
    }
}
```
Run command: `go test -race ./tests/integration/...`  
**Pass Criteria:** Zero race warnings reported by the Go race detector.

---

## C0.2 — Non-Durable Accounts

### 1. The Fundamental Concept
**Durability** means when the database acknowledges a transaction as successful, that state **must persist across crashes, power loss, and restarts**.

If an application creates an account (e.g. Account A with $500 balance), gets a `200 OK` response, and then the server restarts, Account A **must still exist** when the server comes back up.

### 2. Current Code & Root Cause Analysis
Look at [`internal/engine/ledger.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/engine/ledger.go#L134-L144):
```go
func (l *Ledger) CreateAccount(ctx context.Context, acc core.Account) (core.Account, error) {
    if err := l.accounts.Create(acc); err != nil { // MUTATES IN-MEMORY ONLY!
        return core.Account{}, err
    }
    return acc, nil // NO WAL RECORD WRITTEN!
}
```
And look at recovery in [`internal/engine/ledger.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/engine/ledger.go#L167-L181):
```go
func (l *Ledger) Recover() error {
    return l.wal.Recover(func(r wal.Record) error {
        if r.Type != wal.RecordTypeTransfer { // IGNORES ALL NON-TRANSFER RECORDS!
            return nil
        }
        // ...
    })
}
```
Account creation **never writes a WAL record**, and recovery **only replays transfers**!

### 3. Catastrophic Production Failure
1. User creates Account A via API `POST /v1/accounts`.
2. Account A sends $100 to Account B. Transfer succeeds.
3. Node crashes (`kill -9`) or power blinks.
4. On startup, WAL replays transfers. When replaying transfer (A -> B), Account A **does not exist in memory**! Recovery fails with `ErrAccountNotFound` or aborts, leaving the ledger in a broken, non-recoverable state.

### 4. Engineering Mental Model & Solution Design
To make account operations durable:
1. Account creation, freezing, and closure must go through the **Ring Buffer / Event Loop** just like transfers.
2. The event loop must append a `RecordTypeAccount` record to the **WAL** and execute `wal.Sync()`.
3. `wal.Recover()` must handle both `RecordTypeAccount` and `RecordTypeTransfer` in chronological sequence.

### 5. Exact Code Fix (Before vs. After)

#### AFTER: Encode Account Records in WAL and Replay
1. In `internal/wal/record.go`, ensure `EncodeAccount` and `DecodeAccount` exist:
```go
const (
    RecordTypeTransfer byte = 0x01
    RecordTypeAccount  byte = 0x02
)
```

2. In `internal/engine/ledger.go` (Recovery):
```go
func (l *Ledger) Recover() error {
    return l.wal.Recover(func(r wal.Record) error {
        switch r.Type {
        case wal.RecordTypeAccount:
            acc, err := codec.DecodeAccount(r.Payload)
            if err != nil {
                return err
            }
            return l.accounts.Create(acc)
        case wal.RecordTypeTransfer:
            tr, err := codec.DecodeTransfer(r.Payload)
            if err != nil {
                return err
            }
            return l.transfers.Apply(tr)
        default:
            return nil
        }
    })
}
```

### 6. Automated Proof & Verification
Write crash-recovery integration test in `tests/integration/account_durability_test.go`:
```go
func TestAccountDurabilityAcrossCrash(t *testing.T) {
    dir := t.TempDir()
    
    // 1. Start ledger, create account via API
    l1 := setupLedgerAt(t, dir)
    accID := [16]byte{0x01}
    _, err := l1.CreateAccount(ctx, core.Account{ID: accID, Currency: "USD"})
    require.NoError(t, err)
    l1.Close() // Simulate crash
    
    // 2. Restart ledger from same WAL dir
    l2 := setupLedgerAt(t, dir)
    err = l2.Recover()
    require.NoError(t, err)
    
    // 3. Assert account exists
    acc, err := l2.GetAccount(ctx, accID)
    require.NoError(t, err)
    require.Equal(t, "USD", acc.Currency)
}
```

---

## C0.3 — Broken LSN (Log Sequence Number) Accounting

### 1. The Fundamental Concept
A **Log Sequence Number (LSN)** is a monotonically increasing 64-bit integer (`1, 2, 3, 4...`) assigned to log entries. It acts as the ultimate reference point in a log-structured database:
- **LSN identifies progress:** "The engine is currently at LSN 5,420."
- **LSN drives replication:** Follower asks Primary: "Give me all log records after LSN 5,420."
- **LSN enables snapshot safety:** "We snapshotted state at LSN 5,000, so we can delete WAL segments with LSN < 5,000."

### 2. Current Code & Root Cause Analysis
Look at [`internal/wal/wal.go:89`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/wal/wal.go#L89):
```go
func (w *WAL) AppendBatch(records []Record) error {
    // ...
    w.currentLSN++ // <--- INCREMENTS ONCE PER BATCH!
    return nil
}
```
Now look at recovery in [`internal/wal/recovery.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/wal/recovery.go):
```go
func (w *WAL) Recover(fn func(Record) error) error {
    // ...
    recoveredLSN += recs // <--- INCREMENTS ONCE PER RECORD!
}
```
**The Conflict:**
If you write 10 batches containing 50 records each:
- Live process LSN = `10`
- Recovered process LSN = `500`

Furthermore, look at [`internal/wal/wal.go:125-139`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/wal/wal.go#L125-L139):
```go
func (w *WAL) TruncateBefore(lsn uint64) error {
    // Completely IGNORES lsn argument and deletes all non-active segment files!
    for _, seg := range w.segments[:len(w.segments)-1] {
        os.Remove(seg.path)
    }
}
```

### 3. Catastrophic Production Failure
Primary and Follower lose synchronization because their LSNs do not match. If `TruncateBefore` is called after a snapshot, it deletes WAL segments that haven't been snapshotted or replicated yet, causing **permanent data loss**.

### 4. Engineering Mental Model & Solution Design
1. **Define LSN Granularity:** Define LSN as monotonically increasing **per record**. Record 1 = LSN 1, Record 2 = LSN 2...
2. **Embed LSN in Record Header:** Each binary record on disk must carry its assigned LSN:
   `[ LSN (8 bytes) | Type (1 byte) | Length (4 bytes) | Payload | CRC32 (4 bytes) ]`
3. **Segment-Aware Truncation:** `TruncateBefore(targetLSN)` checks each segment's `maxLSN`. A segment is only deleted if its `maxLSN < targetLSN`.

### 5. Exact Code Fix (Before vs. After)

#### AFTER: Per-Record LSN & Segment Bounds
```go
func (w *WAL) AppendBatch(records []Record) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    for i := range records {
        w.currentLSN++
        records[i].LSN = w.currentLSN
        
        encoded := encodeRecord(records[i])
        if err := w.activeSegment.Write(encoded); err != nil {
            return err
        }
    }
    return w.activeSegment.Sync()
}

func (w *WAL) TruncateBefore(lsn uint64) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    var kept []*Segment
    for _, seg := range w.segments {
        if seg.MaxLSN < lsn && seg != w.activeSegment {
            os.Remove(seg.Path) // Safe deletion!
        } else {
            kept = append(kept, seg)
        }
    }
    w.segments = kept
    return nil
}
```

---

## C0.4 — Torn-Batch Replay (Lack of Batch Atomicity)

### 1. The Fundamental Concept
In database engineering, **Group Commit** batches 100 incoming transactions into a single write buffer and issues one `fsync()` to disk. 

What happens if power fails **during** that `fsync()`? The disk may persist bytes 0 through 2,000 of the batch, but fail to write bytes 2,001 through 5,000. This is called a **torn batch**.

Atomicity requires **all-or-nothing**: recovery must either recover **all 100 transfers** in that batch or **0 transfers**. Replaying a partial batch (e.g. applying debits without matching credits) violates the financial conservation invariant!

### 2. Current Code & Root Cause Analysis
Look at [`internal/wal/record.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/wal/record.go). Each record is encoded independently:
`[ Type | Len | Payload | CRC32 ]`

During [`internal/wal/recovery.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/wal/recovery.go), recovery reads record by record. If the 41st record in a batch of 100 passes CRC, but the 42nd is corrupted, recovery stops at 41 and **keeps the 41 records applied**!

### 3. Catastrophic Production Failure
A multi-transfer batch representing a complex multi-party settlement is interrupted mid-write by a node crash. Replay recovers the first half of the settlement but loses the second half. Money is debited from Account A, but never credited to Account B. **Double-entry bookkeeping is broken.**

### 4. Engineering Mental Model & Solution Design
To enforce **batch atomicity**, we wrap the batch in a transaction frame:
- **Approach: Batch Framing Header & Footer**
  - Write a `BatchHeader{BatchID, RecordCount}` before the records.
  - Write a `BatchCommitMarker{BatchID, BatchCRC32}` after all records in the batch are written.
  - During recovery, read ahead to verify the `BatchCommitMarker`. If the commit marker is missing or CRC fails, **discard the entire uncommitted batch batch-tail**.

### 5. Exact Code Fix (Before vs. After)

#### AFTER: Transaction Commit Marker Framing
```go
type BatchCommitRecord struct {
    BatchID     uint64
    RecordCount uint32
    BatchCRC    uint32
}

func (w *WAL) Recover(fn func(Record) error) error {
    records, err := w.readAllRawRecords()
    if err != nil {
        return err
    }

    var pendingBatch []Record
    for _, r := range records {
        if r.Type == RecordTypeBatchCommit {
            // Verify commit marker!
            if len(pendingBatch) == int(r.BatchCommit.RecordCount) {
                for _, validRecord := range pendingBatch {
                    if err := fn(validRecord); err != nil {
                        return err
                    }
                }
            }
            pendingBatch = pendingBatch[:0] // Clear for next batch
            continue
        }
        pendingBatch = append(pendingBatch, r)
    }
    // Any leftover records in pendingBatch without a BatchCommit marker are DISCARDED!
    return nil
}
```

---

## C0.5 — Snapshot Subsystem Unwired & Unsafe

### 1. The Fundamental Concept
As a ledger runs for months, the WAL grows to billions of records. Replaying the log from scratch on startup would take days.

A **Snapshot (Checkpoint)** captures a full, immutable point-in-time image of all account balances at LSN $X$. Once Snapshot $X$ is flushed to disk, all WAL segments with LSN $< X$ can be safely pruned (deleted).

Startup sequence becomes:
1. Load latest Snapshot at LSN $X$.
2. Replay WAL starting **only from LSN $X+1$**.

### 2. Current Code & Root Cause Analysis
The package [`internal/snapshot`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/snapshot) exists, but:
1. It is **never imported** by `cmd/server/main.go` or `internal/engine/ledger.go`.
2. The checkpointer background task is never started.
3. If wired as-is with the existing bug in `TruncateBefore` (C0.3), calling snapshot truncation deletes active WAL segments while recovery still requires them!

### 3. Catastrophic Production Failure
Restarting a production node with a 500 GB WAL takes 4 hours, causing extended service outage. Or, an engineer tries wiring snapshots, triggering `TruncateBefore` which deletes un-snapshotted WAL files, causing **irrecoverable data loss**.

### 4. Engineering Mental Model & Solution Design
1. Fix LSN and `TruncateBefore` first (C0.3).
2. Wire snapshot loading inside `Ledger.Open()` / `Ledger.Recover()`:
   ```
   [Startup] ──> Load Snapshot(LSN_N) ──> Recover WAL(LSN_N+1 ... Latest)
   ```
3. Wire background snapshot creation on an interval (e.g. every 100,000 transfers or 15 minutes) inside `loop.go`.

---

## C0.6 — Non-Durable Idempotency Store

### 1. The Fundamental Concept
**Idempotency** guarantees that performing the same operation multiple times produces the exact same result as performing it once.

Clients pass a unique `idempotency_key` (e.g. UUID `tx_98765`). If a network timeout occurs, the client retries the request with `tx_98765`. The server recognizes `tx_98765`, skips execution, and returns the cached result.

### 2. Current Code & Root Cause Analysis
Look at [`internal/engine/idempotency.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/engine/idempotency.go). The store uses an in-memory Go map:
```go
type IdempotencyStore struct {
    entries map[string]Response
}
```
When the ledger server restarts or crashes, this map is wiped clean!

### 3. Catastrophic Production Failure
1. Client submits payment `$50` with key `req_abc`.
2. Payment processes. Server crashes before client receives response.
3. Server restarts (idempotency map is now empty).
4. Client retries payment `$50` with key `req_abc`.
5. Server executes payment **again**. Client is debited **$100 total** instead of $50!

### 4. Engineering Mental Model & Solution Design
Every transfer WAL record already stores `idempotency_key` inside its payload!
We don't need a separate database for idempotency. During startup WAL recovery (`wal.Recover()`), as we replay each transfer record, we **populate the `IdempotencyStore` map** in memory for all keys within the active TTL window!

#### AFTER: Rebuilding Idempotency Index During Recovery
```go
func (l *Ledger) Recover() error {
    return l.wal.Recover(func(r wal.Record) error {
        if r.Type == wal.RecordTypeTransfer {
            tr, _ := codec.DecodeTransfer(r.Payload)
            l.transfers.Apply(tr)
            
            // Rebuild Idempotency Store!
            if tr.IdempotencyKey != "" {
                l.idempKey.Commit(tr.IdempotencyKey, tr)
            }
        }
        return nil
    })
}
```

---

## C0.7 — Panics Inside the State Machine

### 1. The Fundamental Concept
In Go, `panic()` unwinds the call stack and crashes the process unless caught by `recover()`.

In state machine engineering, **panics should NEVER be triggered by invalid user inputs** (such as insufficient funds, non-existent account ID, or invalid currency). Invalid inputs must return explicit error codes so the API layer can respond with `400 Bad Request`.

### 2. Current Code & Root Cause Analysis
Look at [`internal/core/account.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/core/account.go) and [`internal/engine/accounts.go:78-89`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/engine/accounts.go#L78-L89):
```go
func (am *AccountManager) ApplyDebit(id [16]byte, amount core.Uint128) {
    acc, ok := am.accounts[id]
    if !ok {
        panic("account not found") // <--- PROCESS CRASH!
    }
    if acc.Balance.LessThan(amount) {
        panic("insufficient funds") // <--- PROCESS CRASH!
    }
}
```

### 3. Catastrophic Production Failure
An attacker or buggy client sends an HTTP request requesting a transfer of $999,999 from an account with a $10 balance. `ApplyDebit` panics, taking down the entire database server for all users!

### 4. Engineering Mental Model & Solution Design
Separate **Domain Validation Errors** from **System Invariant Breaches**:
- **Domain Validation:** Return typed errors (`ErrAccountNotFound`, `ErrInsufficientBalance`).
- **System Invariants:** Keep `assertInvariant()` for impossible hardware corruptions (e.g. balance total changing without a transfer).

#### AFTER: Typed Error Handling
```go
func (am *AccountManager) ApplyDebit(id [16]byte, amount core.Uint128) error {
    acc, ok := am.accounts[id]
    if !ok {
        return core.ErrAccountNotFound
    }
    if acc.Balance.LessThan(amount) {
        return core.ErrInsufficientBalance
    }
    acc.Balance = acc.Balance.Sub(amount)
    am.accounts[id] = acc
    return nil
}
```

---

## C0.8 — Unsound Replication Apply

### 1. The Fundamental Concept
In Primary-Follower replication, the Follower must execute state machine updates **identically** to the Primary. If Follower state diverges from Primary state, failover will result in corrupted account balances.

### 2. Current Code & Root Cause Analysis
Look at [`internal/replication/follower.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/internal/replication/follower.go):
```go
if err := f.transfers.Apply(tr); err != nil {
    // If account doesn't exist on follower, AUTO-CREATE IT WITH ZERO BALANCE!
    _ = f.accounts.Create(core.Account{ID: tr.DebitAccountID}) 
}
```
And decode errors are silently ignored (`if err == nil`). Wire network frames have no CRC header checksum!

### 3. Catastrophic Production Failure
Network corruption flips bits on a replication frame. The follower receives a corrupted record, creates phantom accounts with zero balance, swallows the error, and silently diverges from the Primary. When the Primary dies and the Follower is promoted, **account balances are completely corrupted**.

### 4. Engineering Mental Model & Solution Design
1. Add **CRC32 checksum** to every replication network frame `[ LSN | Type | Len | Payload | CRC32 ]`.
2. Follower **never auto-creates accounts**. If replication fails, the follower drops state and triggers a full snapshot re-sync.

---

## C0.9 — Server Unable to Run as Follower

### 1. The Fundamental Concept
To deploy a High Availability (HA) cluster, the server binary (`cmd/server/main.go`) must be capable of starting in either `PRIMARY` or `FOLLOWER` mode based on environment configuration (`LEDGER_ROLE`).

### 2. Current Code & Root Cause Analysis
In [`docker-compose.yml`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/docker-compose.yml), environment variables `LEDGER_ROLE=follower` and `PRIMARY_ADDR=primary:50051` are passed to containers.

However, in [`cmd/server/main.go`](file:///Users/shikharsingh/Downloads/code/go-projects/aequitas-ledger/cmd/server/main.go), **zero Go code reads `LEDGER_ROLE`**! The binary always runs as a standalone Primary. Follower replication code is dead code only exercised in unit tests.

### 3. Solution Design & Wire-up
In `cmd/server/main.go`:
```go
role := os.Getenv("LEDGER_ROLE")
if role == "follower" {
    primaryAddr := os.Getenv("PRIMARY_ADDR")
    follower := replication.NewFollower(primaryAddr, ledgerEngine)
    go follower.Start(ctx)
    // Expose read-only APIs, reject writes with ErrNotLeader
} else {
    // Run as Primary
}
```

---

## C0.10 — Housekeeping, Dead Code & Hygiene

### 1. The Fundamental Concept
Clean repository hygiene ensures that reviewers and engineers can trust the codebase. Dead code, untracked documentation files, and unneeded dependencies erode project credibility.

### 2. Action Items
1. **Track `docs/`:** Run `git add docs/` and commit the ADRs and roadmap.
2. **Remove Ethereum CLI leakage:** Remove third-party `rpc-test` code and `golang.org/x/net/websocket` from `cmd/ledger-cli` and `go.mod`.
3. **Clean Dead Code:** Remove unused functions (`wal.encodeBatch`, `replication.dummyWriter`).
4. **Setup CI Pipeline:** Configure GitHub Actions for `go test -race ./...` and `golangci-lint`.

---

# Summary & Next Steps
With these detailed technical guides, we are ready to methodically resolve each Tier 0 issue step-by-step in code.
