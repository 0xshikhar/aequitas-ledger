# Phase 1 — Write-Ahead Log (WAL) Durability Layer Deep-Dive

## 1. Objective & Problem Statement

In crash-safe financial ledgers, in-memory state can be lost instantly upon sudden process crash or power outage. To guarantee durability (the **D** in ACID), every state mutation must be written to disk in a write-ahead log (WAL) before updating in-memory balances.

Key operational requirements for the WAL:
1. **Append-only sequential IO**: Rotating segmented log files (`wal-000001.seg`, `wal-000002.seg`) to maximize NVMe/SSD sequential write speeds.
2. **Group Commit (`fsync`) primitives**: Batching multiple incoming transactions into a single disk sync operation (`fsync`/`fdatasync`) to overcome disk physical write bottleneck (~100-1000 fsync/sec limit).
3. **Crash Recovery & Tail Truncation**: Detecting ungraceful mid-write power cuts or corrupted trailing bytes using CRC32 checksums and cleanly truncating damaged file ends.

---

## 2. Core Go Language Concepts & Engineering Internals

### A. Segment File Pre-Allocation & Zero-Allocation Appends
Creating disk files dynamically during write paths introduces filesystem metadata locks. The WAL uses segment pre-allocation (`openSegment`):

```go
type Segment struct {
    id       int
    file     *os.File
    offset   int64
    maxBytes int64
}
```

When appending a record batch:
```go
func (w *WAL) AppendBatch(records []Record) (int64, error) {
    startLSN := w.currentLSN + 1
    for _, rec := range records {
        encoded, _ := EncodeRecord(rec)
        if w.current.IsFull(len(encoded)) {
            if err := w.Rotate(); err != nil {
                return 0, err
            }
        }
        _, err := w.current.Write(encoded)
        if err != nil {
            return 0, err
        }
    }
    w.currentLSN++
    return startLSN, nil
}
```

* **Go Concept (`os.File` & `fsync`)**: Calling `file.Write()` writes bytes to OS page cache (VFS buffer). It is **not** written to physical media until `file.Sync()` (`fsync` system call) executes.

---

### B. Group Commit Pattern
Calling `fsync` after every single transfer drops throughput down to ~200 TPS because SSD/NVMe disk controllers can only perform a limited number of physical sync commands per second.

```
Individual Sync (Naive):
Tx 1 ──> Append ──> fsync (wait 5ms) ──> Ack
Tx 2 ──> Append ──> fsync (wait 5ms) ──> Ack   => Total TPS ≈ 200

Group Commit (Aequitas):
Tx 1 ──┐
Tx 2 ──┼──> AppendBatch ──> Single fsync (wait 5ms) ──> Ack All
Tx 3 ──┘                                               => Total TPS > 50,000
```

---

### C. Corruption Detection & Recovery Traversal
During startup recovery, `recoverSegments` scans all `.seg` files sequentially from segment `000001`:

```go
func recoverSegments(dir string, handler func(Record) error) (int64, int, error) {
    // 1. Discover all segment files in sorted order
    // 2. Open segment file and read sequentially
    // 3. For each frame, verify CRC32
    // 4. If partial frame / truncated tail encountered at EOF:
    //    Truncate file at last known valid offset (repair damaged tail)
}
```

* **Go Concept (`io.ReadFull`)**: Ensures exact byte counts are read into reusable buffers, avoiding slice reallocation during log scanning.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **Storage Model** | Segmented Append-Only Log | SQLite / B-Tree Database | B-Trees suffer random disk write amplification; append-only logs maintain maximum sequential write performance. |
| **Sync Strategy** | Group Commit `Sync()` | `O_SYNC` flag per write | `O_SYNC` forces every syscall to block on disk flush; Group Commit amortizes disk latency over thousands of requests. |
| **Corruption Handling** | Automatic Tail Truncation | Abort on corrupt tail | Partial trailing writes occur naturally during sudden power cuts; truncating damaged tails guarantees clean deterministic restart. |
