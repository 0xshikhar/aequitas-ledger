# ADR 002 — WAL group commit

**Status:** Accepted  
**Bucket:** 1 — Core write mechanics  
**Affects:** `internal/wal/wal.go`, `internal/engine/loop.go`

---

## Context

A write-ahead log (WAL) provides the durability guarantee: if the process crashes, the WAL can be replayed on restart to reconstruct the exact state that was committed. The WAL is only useful if it is flushed to durable storage before the transfer is acknowledged to the client. That flush is the `fsync` system call.

`fsync` is the single most expensive operation in the write path. On enterprise NVMe drives it takes 50–200µs. On cloud block storage (EBS, GCP PD) it can take 1–5ms. Calling it once per transfer means your maximum TPS is:

```
max_TPS = 1 / fsync_latency
         = 1 / 0.0001s          (100µs NVMe)
         = 10,000 TPS
```

No amount of goroutine tuning, in-memory optimisation, or hardware upgrade can break this ceiling if fsync is called per transfer.

---

## Decision

Implement **group commit**: the single-writer loop accumulates a batch of transfers, writes all records to the WAL segment in a single contiguous write, then calls `fsync` **exactly once** for the entire batch before acknowledging any transfer in the batch.

The amortised I/O cost per transfer becomes:

```
cost_per_transfer = fsync_latency / batch_size
                  = 100µs / 10,000
                  = 0.01µs per transfer
```

The ceiling becomes:

```
max_TPS ≈ batch_size / fsync_latency
        = 10,000 / 0.0001s
        = 100,000,000 TPS  (ceiling is now CPU/memory, not disk)
```

### WAL API shape

```go
// internal/wal/wal.go

// AppendBatch serialises all records into one contiguous write.
// Does NOT call fsync — the loop controls when to flush.
func (w *WAL) AppendBatch(records []Record) (startLSN int64, err error)

// Sync calls fdatasync on the current segment file.
// Called exactly once per batch, after AppendBatch.
func (w *WAL) Sync() error

// Recover replays all segments in LSN order, calling handler per record.
// Called once at startup before the event loop starts.
func (w *WAL) Recover(handler func(Record) error) error
```

### Record format

```
[ type: 1 byte ][ length: 4 bytes ][ payload: N bytes ][ crc32: 4 bytes ]
```

- `type` distinguishes `RecordTypeTransfer = 1`, `RecordTypeAccountCreate = 2`, `RecordTypeCheckpoint = 3`
- `length` is the byte length of `payload` only
- `crc32` covers `type + length + payload` — detected on recovery, returns `ErrCorrupted`
- LSN (Log Sequence Number) is a monotonically increasing `int64` tracked in the WAL header, incremented per `AppendBatch` call

### Segment files

WAL is split into fixed-size segment files (`wal-000001.seg`, `wal-000002.seg`, …).

- Default segment size: 64MB
- When a segment is full, `Rotate()` seals it (writes segment footer with final LSN) and opens a new one
- Recovery iterates segments in numeric order
- Old segments are retained until the checkpointer confirms a snapshot has been written past their LSN — only then are they safe to delete

---

## The batch timeout knob

The batch collector in `batch.go` controls the group commit cadence:

```go
// Collect blocks until either:
//   - the ring buffer has >= maxBatchSize events, OR
//   - batchTimeout has elapsed since the first event arrived
// Returns all available events, up to maxBatchSize.
func (b *Batcher) Collect() []TransferEvent
```

This is the primary throughput/latency tradeoff lever:

| `batchTimeout` | Behaviour |
|---|---|
| 0ms | Flush immediately when any event arrives — lowest latency, lowest throughput |
| 1ms | Accumulate up to 1ms of events — balanced |
| 5ms | Maximise batch size, maximise throughput, worst tail latency |

**Default:** 1ms. Expose as `LEDGER_BATCH_TIMEOUT_MS` environment variable. Benchmark all three values and include results in `bench/report.go`.

---

## Durability guarantee

**What we guarantee:** Any transfer that has received a `nil` error response has been written to the WAL and fsynced. If the process crashes and restarts, WAL replay will reconstruct that transfer.

**What we do not guarantee:** Transfers that were in-flight (submitted to the ring buffer but not yet part of a completed batch) when a crash occurs will not be recovered. The gRPC handler will receive `ErrConnectionReset` and the client must retry with the same idempotency key.

This is an **at-least-once** delivery model at the WAL level combined with **idempotent application** at the state-machine level. It is the same model used by Kafka, Postgres replication, and most production financial systems.

---

## Crash mid-batch recovery

The final batch before a crash may be partially written to the segment file. Recovery must handle this.

`recovery.go` implements:

```go
// detectTruncation scans a segment from the end, walking records backwards
// to find the last complete record (valid CRC). Returns the byte offset
// of the last complete record's end.
func detectTruncation(seg *Segment) (lastValidOffset int64, err error)
```

On recovery:
1. Open all segments in LSN order
2. For each segment, scan records sequentially, verifying CRC
3. On the first CRC failure, call `detectTruncation` — everything after `lastValidOffset` is truncated and ignored
4. Replay all valid records into the state machine
5. Truncate the segment file at `lastValidOffset` before starting the write loop

---

## Alternatives considered

**Per-transfer fsync (rejected).** Correct but caps TPS at NVMe IOPS — approximately 10k–50k TPS. Chosen as the Postgres baseline for benchmark comparison.

**`O_DSYNC` writes without explicit fsync (rejected).** On Linux, `O_DSYNC` flushes each write synchronously at the kernel level. It avoids the explicit `fdatasync` call but does not allow batching — same ceiling as per-transfer fsync. Not portable to macOS.

**Async WAL with background fsync thread (rejected for v1).** Write to WAL asynchronously, fsync in a background goroutine on a fixed interval (e.g., every 100ms). Dramatically higher throughput but degrades the durability guarantee — up to 100ms of committed transfers could be lost on crash. Valid for an analytics or CDC path but not for the primary ledger write path.

---

## Key files

| File | Role |
|---|---|
| `internal/wal/wal.go` | `Open`, `AppendBatch`, `Sync`, `Recover`, `Rotate`, `Close` |
| `internal/wal/segment.go` | Fixed-size file management, `Write`, `ReadAt`, `IsFull` |
| `internal/wal/record.go` | `EncodeRecord`, `DecodeRecord`, CRC32 verification |
| `internal/wal/recovery.go` | `Recover`, `detectTruncation`, `replayRecord` |
| `internal/engine/batch.go` | `Collect` — the group commit cadence controller |
