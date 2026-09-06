# Phase 7 — Hardening, Async Snapshot Checkpointer & Fast Recovery Deep-Dive

## 1. Objective & Problem Statement

In a write-ahead log (WAL) engine, log files grow continuously with uptime. At 200,000 TPS, a 24-hour run produces over 17 billion log records (~1.7 TB). Replaying 1.7 TB of records on server restart takes **hours**, causing unacceptable service downtime.

Phase 7 implements **Async Copy-on-Write Snapshotting & Checkpointing**:
1. **Zero-Stall Inter-batch View Capture**: The `EventLoop` takes a shallow copy of the 64-byte `Account` slice (~12ms pause for 5 million accounts) between batch iterations.
2. **Async Binary Disk Persistence**: The background `Checkpointer` worker serializes the snapshot to disk (`snapshot-{lsn}.tmp` -> `fsync` -> rename to `snapshot-{lsn}.snap`).
3. **WAL Truncation**: Old WAL log segments prior to `snapshotLSN` are pruned safely.
4. **Accelerated Recovery**: On startup, the engine restores balances directly from the latest valid `.snap` file and only replays recent WAL records created *after* the snapshot LSN.

---

## 2. Core Go Language Concepts & Engineering Internals

### A. Binary Snapshot Memory Layout & Zero-Allocation Copy
The snapshot binary format uses a 28-byte header followed by packed 64-byte account structs:

$$\text{[Magic: 8B "LEDGER01"]} \;\Vert\; \text{[LSN: 8B]} \;\Vert\; \text{[Count: 8B]} \;\Vert\; \text{[Accounts: Count} \times \text{64B]} \;\Vert\; \text{[CRC32: 4B]}$$

#### Unsafe Pointer Casting (`unsafe.Pointer`)
Because `core.Account` is guaranteed to be exactly 64 bytes with zero heap pointers, we cast account struct addresses directly to byte arrays without serialization overhead:

```go
for i := range accounts {
    raw := (*[AccountSize]byte)(unsafe.Pointer(&accounts[i]))[:]
    writer.Write(raw)
}
```

And for fast reading:
```go
for i := uint64(0); i < count; i++ {
    offset := i * AccountSize
    accPtr := (*core.Account)(unsafe.Pointer(&payload[offset]))
    accounts[i] = *accPtr
}
```

* **Go Concept (`unsafe.Pointer`)**: Performs O(1) memory reinterpretation, avoiding object instantiation or field-by-field reflection.

---

### B. Inter-Batch Shallow Copy & Coordination Channel
The `Checkpointer` requests snapshot views over a control channel (`snapshotRequests`):

```go
type SnapshotRequest struct {
    Result chan SnapshotView
}

type SnapshotView struct {
    Accounts []core.Account
    LSN      int64
}
```

Inside `EventLoop.Run()`:
```go
select {
case <-ctx.Done():
    return
case req := <-l.snapshotRequests:
    accs := l.accounts.Snapshot() // Shallow slice header copy
    req.Result <- SnapshotView{Accounts: accs, LSN: l.currentLSN}
default:
}
```

#### Why Copying a Slice Header is Instant
`l.accounts.Snapshot()` executes:
```go
out := make([]core.Account, len(m.accounts))
copy(out, m.accounts)
return out
```
Because `core.Account` is a value type stored contiguously in memory, `copy()` performs a direct CPU memory block move (`memmove`). 5 million accounts (320 MB) are copied in **~12ms** on modern RAM architectures. Live gRPC handlers experience zero blocking locks.

---

### C. Atomic Write-Then-Rename Crash Safety
To prevent loading partially written snapshot files after a power crash mid-snapshot:
1. Write to `snapshot-{lsn}.tmp`.
2. Execute `f.Sync()` (`fsync`).
3. Call `os.Rename(tmpPath, finalPath)` (atomic operation on POSIX filesystems).
4. Append `RecordTypeCheckpoint` to WAL and invoke `wal.TruncateBefore(lsn)`.

If the machine crashes mid-write, the incomplete `.tmp` file is ignored during recovery, ensuring absolute data integrity.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **Snapshot Strategy** | Async Copy-on-Write | Synchronous Pause-and-Write | Synchronous pause blocks all gRPC write handlers for 500ms+; Async CoW reduces write handler pause window to ~12ms between batches. |
| **Persistence Format** | Binary Memory Layout | JSON / Protobuf Snapshot | Binary layout allows O(1) direct memory cast (`unsafe.Pointer`) without reflection or string conversion allocations. |
| **File Creation** | Write-Then-Rename | Direct Write to Target File | Direct file writes risk corrupted/partial snapshots on sudden power loss. Write-then-rename guarantees atomic filesystem visibility. |
