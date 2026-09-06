# 04 — File I/O, fsync & Durability

**Files in focus:** `internal/wal/wal.go`, `internal/wal/segment.go`,
`internal/wal/recovery.go`, `internal/snapshot/snapshot.go`

This document is about the boundary where Go stops being a language and
starts being an operating system client: what your process promises versus
what the disk actually has after the power fails. Every financial guarantee
this system makes reduces to one question — *when `Sync()` returns, is it
durable, and what exactly became durable?*

---

## 1. The write path: what each layer guarantees

```go
// internal/wal/segment.go
f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)

func (s *Segment) Write(p []byte) (int64, error) {
    ...
    start := s.writeOffset
    n, err := s.file.WriteAt(p, start)
    s.writeOffset += int64(n)
    ...
}
```

`WriteAt` (positional write, no shared file offset — important if any other
goroutine ever touches the file) hands bytes to the **page cache**: kernel
memory, not disk. The data survives your process crashing. It does *not*
survive the machine losing power. Only `Sync()` (`fsync`) forces the kernel
to flush the dirty pages (and the file's metadata) to the device, and only
when `Sync()` returns does "the transfer was acked" mean anything.

That is the entire group-commit design in one sentence: `AppendBatch` writes
into the page cache (fast, ~µs), and the loop calls `Sync()` **once per
batch** (`loop.go processBatch`), so one physical flush amortizes over every
transfer in the batch. Per-record fsync would cap throughput at the device's
fsync rate (spinning disks: ~120/s; even NVMe: ~10–100k/s); batching turns
the ceiling into transfers-per-flush × flush-rate.

Current state and the deliberate gaps (roadmap S2.2):

- No `O_DIRECT`: the kernel page cache sits in between, which also means a
  sync may not hit platters if the device has a volatile write cache.
- No `fallocate` preallocation: each `Rotate` grows the file through the
  filesystem, which costs metadata updates and fragmentation mid-write.
- `file.Sync()` on Linux is `fsync` (metadata + data); `fdatasync` would be
  cheaper for appends that don't change file size semantics.

## 2. Segments and rotation

The WAL is a sequence of numbered files (`wal-000001.seg`, ...), each capped
(`DefaultSegmentSize = 64 MiB`). Rotation is checked before every record:

```go
if w.current.IsFull(len(encoded)) {
    if err := w.Rotate(); err != nil { return 0, err }
}
```

`Rotate` syncs and closes the old segment, opens the next. **A batch can
straddle a rotation**: its records land at the end of segment *k* and its
commit record in segment *k+1*. Hold that thought — it is the seed of the
worst bug this project fixed (§5).

## 3. Recovery: reading what crashes left behind

```go
// internal/wal/recovery.go
func recoverSegments(dir string, fromLSN int64, handler func(Record) error) (...)
```

Recovery walks segments in ID order, frames them into records
(`[LSN|type|len|payload|crc]`), buffers each batch's records, and invokes the
handler only when a valid `BatchCommit` marker with a matching count arrives
— uncommitted tails are crash debris, not data. Three rules the
implementation encodes, each earned:

**Rule 1 — replay stops at the first untrustworthy byte.** A short header, a
frame longer than the file, a CRC mismatch: everything after it is discarded
(the segment is truncated there, later segments deleted as stale). The
reason is that a torn write can leave *plausible-looking* garbage; the only
safe boundary is the last byte you can prove.

**Rule 2 — batch state spans segments** (bug C0.17). The first
implementation kept `pendingBatch` inside `replaySegment`, one call per
segment. A batch whose records ended segment *k* and whose commit began
segment *k+1* looked like an uncommitted tail: recovery truncated it *and
deleted the following segments as stale*. That is silent loss of committed,
client-acked transfers. The fix moved batch state up to `recoverSegments` so
it survives the boundary:

```go
type replayState struct {
    pending    []Record
    highestLSN int64
}
```

The lesson is bigger than WAL: **when a state machine's input is chunked,
its state must live above the chunking.** Any "per-chunk" mutable state is a
bug the first time a logical item spans two chunks.

**Rule 3 — delta replay must not re-read history** (C0.13). With a snapshot
at LSN S, segments entirely below S are peek-skipped (§4) and the replay
handler filters `LSN <= S`. The recovered watermark is floored at S: the
caller *asserts* state through S, so the ledger comes up at S even if the
surviving WAL is shorter than the snapshot's history — and new records can
never reuse LSNs the snapshot already vouches for (`AdvanceLSNTo`).

## 4. Reading a tail without reading the segment (C0.14)

`TruncateBefore(snapshotLSN)` deletes old segments after a snapshot. To know
a segment is fully below the cut, it needs the segment's max LSN. Reading
the whole 64 MiB file for that — per segment, per snapshot cycle — is 64 MiB
of I/O per minute per segment, forever. The fix reads a 64 KiB tail chunk
and resyncs into the frame chain:

```go
buf := make([]byte, chunk)
f.ReadAt(buf, size-chunk)
for start := 0; start < limit; start++ {
    end, lsn := walkFrameChain(buf[start:])
    if end == len(buf)-start { break }   // chain reaches EOF: real last frame
}
```

The chunk usually starts *mid-frame*, so it tries successive start offsets
until a chain of structurally valid frames walks exactly to EOF — then the
last frame's LSN is the answer. Structural validity (lengths, nonzero type,
positive LSN) is enough for *this* purpose; integrity (CRC) stays
recovery's job. Note the Go details: `ReadAt` returns `io.EOF` when it reads
fewer bytes than requested (handled explicitly), and `os.Stat` sizes the
read. An allocation-bound test
(`TestTruncateBeforeDoesNotReadWholeSegments`) proves the invariant with
`runtime.MemStats` — document 07 shows how.

Also note `TruncateBefore`'s contract, now written into its doc comment: the
cut LSN must be a **committed-batch boundary** (a snapshot LSN). Cutting
mid-batch orphans one side of the batch across the deletion boundary — the
same class of bug as C0.17, invited by the API's own misuse.

## 5. Snapshots: the atomic-rename pattern

```go
// internal/snapshot/snapshot.go
tmpPath := fmt.Sprintf("%s.tmp-%d", path, timeNowNano())
f, _ := os.Create(tmpPath)
... write magic, LSN, count, accounts, CRC ...
f.Sync()
f.Close()
return os.Rename(tmpPath, path)
```

You can never update a file in place atomically. The pattern is:
write a temp file, `Sync()` it, `os.Rename` over the target. POSIX rename
within a directory is atomic — readers see either the old file or the new
one, never a half-written one. (Strictly, durability of the *rename* itself
needs a directory fsync — a refinement on the roadmap.) `CleanupOldSnapshots`
keeps the newest N so a corrupt snapshot still leaves predecessors.

The pairing rule that makes truncation safe: **only delete WAL segments
below a snapshot LSN that has been proven loadable** —
`TestSnapshotAuthoritativeWhenWALShorter` deletes every segment after a
snapshot and proves the ledger still recovers. Order of operations is the
whole game in durability: snapshot first, truncate second, never the
reverse.

## 6. Go file API details worth knowing

- `os.File.WriteAt` vs `Write`: `Write` advances an internal offset shared
  by all goroutines using the file — a data race under concurrency. `WriteAt`
  is position-explicit and safe for concurrent use on the same handle.
- Short writes: `io.Writer` contract allows `n < len(p)` with `err == nil`
  only at EOF; `WriteAt` to a regular file returns a real error, but the
  segment still checks `n != len(p)` defensively (`ioErrShortWrite`).
- `ReadFull` vs `Read`: `io.ReadFull` loops until the buffer is full or a
  non-EOF error; a bare `Read` may return fewer bytes than asked. Recovery
  and replication both use `ReadFull` for framing.
- Closing: `defer f.Close()` in the snapshot writer runs *after* the explicit
  `Close()`+`Rename` succeeded — the deferred close then fails harmlessly
  (the double-close error is ignored) while guaranteeing no leak on error
  paths. The `defer os.Remove(tmpPath)` similarly only bites on failure
  paths, since rename consumed the name on success.

> **Exercise 1.** Write a test that truncates a WAL segment at *every byte
> offset* of a 3-record batch and asserts recovery yields either all 3 or
> none. (This is T3.1 in the roadmap — the property test that would have
> caught C0.17 mechanically.)
>
> **Exercise 2.** Run `TestRecoverBatchStraddlingSegmentRotation` against the
> pre-fix recovery (keep `pendingBatch` local to `replaySegment`) and watch
> 400 committed records become 300. Data-loss bugs look like passing tests
> until the workload rotates segments.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Page cache vs fsync | `WriteAt` then `Sync()` | Acked ⇒ fsynced; nothing weaker |
| Group commit | one `Sync()` per batch | Amortize the flush, never the ack |
| Stop-at-corruption | `replaySegment` | Truncate at the last provable byte |
| State above chunking | `replayState` (C0.17) | Per-chunk state = latent data loss |
| Snapshot authority | floor at snapshot LSN | Never reuse LSNs below the snapshot |
| Tail peeks | 64 KiB + frame resync | Bounded reads for metadata questions |
| Atomic rename | snapshot writes | Temp → fsync → rename; truncate only after proof |
