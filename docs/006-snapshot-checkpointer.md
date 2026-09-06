# ADR 006 — Async snapshot checkpointer

**Status:** Accepted  
**Bucket:** 2 — Operational realities  
**Affects:** `internal/snapshot/checkpointer.go`, `internal/snapshot/snapshot.go`, `internal/wal/wal.go`

---

## Context

The WAL is an append-only log. Every transfer ever processed is stored as a record. On a fresh startup, WAL recovery replays every record from the beginning of time to reconstruct the current account balances.

At 200k TPS, running for 24 hours:

```
records per day = 200,000 × 86,400 = 17,280,000,000 records
record size ≈ 100 bytes
WAL size per day ≈ 1.7 TB
replay time at 1M records/s ≈ 17,280 seconds ≈ 4.8 hours
```

A 4.8-hour recovery window is unusable. The server is down for nearly 5 hours after every restart. This is the problem the checkpointer solves.

### The naive "pause and snapshot" trap

The obvious fix is to periodically pause the event loop, serialise the current `AccountManager` state to disk, and then tell the WAL it can delete all records before the snapshot's LSN.

This works correctly but has a catastrophic latency profile:

- The write loop processes 200k TPS
- Serialising 5M accounts at ~100 bytes each = 500MB
- Writing 500MB to disk at 1 GB/s = 500ms
- **Every gRPC handler in the system blocks for 500ms** while the loop is paused

This is unacceptable for a financial system. A 500ms stall causes client timeouts, cascading retries, and buffer backpressure.

---

## Decision

Implement an **async snapshot** that takes a point-in-time copy of the account state without pausing the write loop.

### Mechanism: copy-on-write snapshot

The key insight is that the single-writer loop is the only goroutine that mutates account state. We can safely take a snapshot by:

1. **Atomically capturing the current LSN.** The loop records `snapshotLSN = currentLSN` at the moment snapshotting begins.
2. **Handing a read-only view to the checkpointer goroutine.** The checkpointer gets a copy (or read-only reference) of the accounts slice at `snapshotLSN`.
3. **Continuing to write.** The write loop continues processing new transfers normally — it does not pause.
4. **Writing the snapshot asynchronously.** The checkpointer goroutine serialises the account state to disk at its own pace.
5. **Recording the checkpoint in the WAL.** Once the snapshot file is durably written, the checkpointer appends a `RecordTypeCheckpoint{snapshotLSN}` to the WAL.
6. **Truncating old WAL segments.** The WAL can now safely delete all segments with a final LSN ≤ `snapshotLSN`.

### Implementation in Go

Because the write loop is single-threaded, capturing a consistent view is straightforward:

```go
// internal/snapshot/checkpointer.go

func (c *Checkpointer) Start(interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        for range ticker.C {
            c.takeSnapshot()
        }
    }()
}

func (c *Checkpointer) takeSnapshot() error {
    // Step 1: Request a snapshot view from the write loop.
    // The write loop sends back a shallow copy of the accounts slice
    // and the current LSN, then immediately continues processing.
    view, lsn := c.loop.RequestSnapshotView()

    // Step 2: Serialise the view to a temp file (async — write loop not paused).
    tmpPath := fmt.Sprintf("%s/snapshot-%d.tmp", c.dir, lsn)
    if err := snapshot.Write(tmpPath, view); err != nil {
        return err
    }

    // Step 3: Atomically rename temp file to final path (crash-safe).
    finalPath := fmt.Sprintf("%s/snapshot-%d.snap", c.dir, lsn)
    if err := os.Rename(tmpPath, finalPath); err != nil {
        return err
    }

    // Step 4: Append checkpoint record to WAL.
    c.wal.AppendCheckpoint(lsn)
    c.wal.Sync()

    // Step 5: Signal WAL to truncate segments before this LSN.
    c.wal.TruncateBefore(lsn)

    return nil
}
```

### RequestSnapshotView — the one coordination point

The only interaction between the checkpointer and the write loop is `RequestSnapshotView()`. This must be designed carefully:

```go
// internal/engine/loop.go

type snapshotRequest struct {
    result chan snapshotView
}

type snapshotView struct {
    accounts []core.Account // shallow copy of the accounts slice
    lsn      int64
}

// RequestSnapshotView is called by the checkpointer goroutine.
// It sends a request to the loop's control channel and blocks briefly
// while the loop takes a shallow copy of the accounts slice.
func (l *EventLoop) RequestSnapshotView() ([]core.Account, int64) {
    req := snapshotRequest{result: make(chan snapshotView, 1)}
    l.snapshotRequests <- req   // buffered channel, non-blocking
    view := <-req.result
    return view.accounts, view.lsn
}
```

The write loop handles this between batches (not mid-batch):

```go
// internal/engine/loop.go — inside Run()

for {
    // Handle any pending snapshot requests between batches
    select {
    case req := <-l.snapshotRequests:
        // Shallow copy: copy the accounts slice header + contents
        // Account is a value type, so this is a true copy, not a reference copy
        accountsCopy := make([]core.Account, len(l.accounts))
        copy(accountsCopy, l.accounts)
        req.result <- snapshotView{accounts: accountsCopy, lsn: l.currentLSN}
    default:
    }

    // Normal batch processing
    batch := l.batcher.Collect()
    if len(batch) == 0 {
        continue
    }
    l.processBatch(batch)
}
```

**Pause duration:** The shallow copy of 5M accounts at 128 bytes each = 640MB copy. At memory bandwidth of ~50 GB/s, this takes approximately **12ms**. This is the only pause, and it happens between batches (not mid-batch). A 12ms pause is acceptable for a snapshot operation that happens every 60 seconds.

If 12ms is too long, the copy can be further optimised by using a generation-based copy-on-write map (accounts that haven't changed since the last snapshot are shared by reference). This is a v2 optimisation.

---

## Snapshot file format

```
[ magic: 8 bytes "LEDGER01" ]
[ snapshot_lsn: 8 bytes int64 ]
[ account_count: 8 bytes int64 ]
[ accounts: account_count × sizeof(core.Account) bytes ]
[ crc32_checksum: 4 bytes ]  // over all bytes except this field
```

Binary format (not JSON/protobuf) to allow O(1) account count read and efficient memory mapping on recovery.

### internal/snapshot/snapshot.go

```
Write(path string, accounts []core.Account) error
    — writes magic, LSN, count, accounts array, CRC to path

Read(path string) ([]core.Account, int64 /*lsn*/, error)
    — reads and validates CRC, returns accounts slice and LSN

Latest(dir string) (path string, lsn int64, err error)
    — finds the most recent .snap file in dir by LSN
```

---

## Recovery sequence on startup

```
cmd/server/main.go → recover():

1. Find latest .snap file in snapshot directory
2. If found:
   a. Read snapshot → load accounts into AccountManager
   b. Record snapshotLSN
3. Open WAL
4. Replay WAL records with LSN > snapshotLSN only
   (earlier records already captured in snapshot)
5. Start event loop
```

This reduces recovery time from "replay everything since forever" to "replay since last snapshot." At a 60-second snapshot interval, worst-case replay covers 60 seconds of WAL records:

```
records in 60s at 200k TPS = 12,000,000 records
replay time at 1M records/s = 12 seconds
```

12 seconds of recovery time is acceptable.

---

## Crash safety of the snapshot process

The snapshot write uses a write-then-rename pattern:

1. Write to `snapshot-{lsn}.tmp`
2. `fsync` the temp file
3. `os.Rename` to `snapshot-{lsn}.snap` (atomic on POSIX filesystems)
4. Only then append the checkpoint record to the WAL

If the process crashes between steps 1 and 3, the `.tmp` file is incomplete and ignored on recovery. If it crashes after step 3 but before step 4, the snapshot exists but the WAL doesn't know about it. On the next startup:
- The snapshot directory scan finds `snapshot-{lsn}.snap`
- Recovery uses it as the starting point
- WAL replay from `lsn` forward reconstructs any transfers processed after the snapshot

The checkpoint WAL record is an optimisation (allows WAL truncation) but is not required for correctness. Correctness comes from the snapshot file existing.

---

## Configuration

```go
// internal/config/config.go
type SnapshotConfig struct {
    Dir             string        // path to snapshot directory
    Interval        time.Duration // default: 60s
    MaxSnapshotsKept int          // delete old snapshots, default: 3
}
```

---

## Alternatives considered

**Pause-and-snapshot (rejected).** Correct but causes 500ms+ stall at scale. Unacceptable for a financial API.

**LMDB/mmap-based storage with OS copy-on-write (deferred).** The OS can take a CoW snapshot of an mmap'd file at zero cost using `fork()`. This is how some production databases snapshot without pausing. Complex to implement correctly in Go due to `fork()` + goroutine interaction. Valid v2 path if 12ms copy pause proves unacceptable.

**No snapshots, retain all WAL segments (rejected).** Correct but recovery time grows unboundedly with uptime. Disk usage also grows unboundedly.

**Streaming replication as primary recovery path (deferred to v2).** In a HA deployment, a replica replaying the WAL in real time eliminates the startup recovery problem — the replica is always warm. This is the production architecture (see ADR 001 notes on multi-partition design). For v1, snapshot-based recovery is sufficient.
