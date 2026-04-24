package unit

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"aequitas-ledger/internal/wal"
)

// buildMultiSegmentWAL writes nBatches batches of batchSize synthetic transfer
// records with realistic 108-byte payloads across multiple small segments and
// returns the pristine directory path plus the committed head LSN.
func buildMultiSegmentWAL(t testing.TB, nBatches, batchSize, segmentSize int) (string, int64) {
	t.Helper()
	dir := t.TempDir()
	w, err := wal.Open(dir, int64(segmentSize))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	payload := make([]byte, 108)
	for b := 0; b < nBatches; b++ {
		recs := make([]wal.Record, batchSize)
		for i := range recs {
			payload[0], payload[1] = byte(b), byte(i)
			recs[i] = wal.Record{Type: wal.RecordTypeTransfer, Payload: append([]byte(nil), payload...)}
		}
		if _, err := w.AppendBatch(recs); err != nil {
			t.Fatalf("append batch %d: %v", b, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	head := w.CurrentLSN()
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return dir, head
}

func copyWALDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	if err := os.CopyFS(filepath.Join(dst, "wal"), os.DirFS(src)); err != nil {
		t.Fatalf("copy wal dir: %v", err)
	}
	return filepath.Join(dst, "wal")
}

func countTransfers(t *testing.T, w *wal.WAL, fromLSN int64) (int, []uint64) {
	t.Helper()
	var lsns []uint64
	n := 0
	err := w.RecoverFromLSN(fromLSN, func(r wal.Record) error {
		if r.Type == wal.RecordTypeTransfer {
			n++
			lsns = append(lsns, r.LSN)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("recover from %d: %v", fromLSN, err)
	}
	return n, lsns
}

// C0.13: a snapshot LSN makes recovery a pure delta replay. Resuming from the
// committed head must hand zero records to the replay handler; resuming from a
// mid LSN must hand exactly the transfers above it; resuming from 0 replays
// everything. The recovered watermark must equal the true head in all cases.
func TestRecoverFromLSNDeltaReplayOnly(t *testing.T) {
	const nBatches, batchSize = 6, 5 // 30 transfers + 6 commits = head LSN 36
	src, head := buildMultiSegmentWAL(t, nBatches, batchSize, 512)

	// Transfers sit at LSNs 1-5, 7-11, 13-17, 19-23, 25-29, 31-35 (commits at
	// 6, 12, 18, 24, 30, 36). Resuming from 13 replays the 19 above it.
	for _, tc := range []struct {
		name     string
		fromLSN  int64
		wantSeen int
	}{
		{"resume at head replays nothing", head, 0},
		{"resume at zero replays everything", 0, nBatches * batchSize},
		{"resume mid-history replays only the delta", 13, 19},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, err := wal.Open(copyWALDir(t, src), 512)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer w.Close()

			got, lsns := countTransfers(t, w, tc.fromLSN)
			if got != tc.wantSeen {
				t.Fatalf("handler saw %d transfer records, want %d", got, tc.wantSeen)
			}
			for _, lsn := range lsns {
				if int64(lsn) <= tc.fromLSN {
					t.Fatalf("handler saw LSN %d at/below resume LSN %d", lsn, tc.fromLSN)
				}
			}
			if w.CurrentLSN() != head {
				t.Fatalf("recovered watermark = %d, want head %d", w.CurrentLSN(), head)
			}
		})
	}
}

// Regression: a batch whose records straddle a segment rotation boundary must
// survive recovery. Its records sit in one segment and its BatchCommit record
// in the next; treating batch state per-segment would misread it as a torn
// tail, truncate it, and delete the later segments — silently dropping a
// batch that was fsynced and acked to clients.
func TestRecoverBatchStraddlingSegmentRotation(t *testing.T) {
	// Segment size tuned so the first batch's records fill segment 1 and its
	// commit record lands in segment 2. ~125 B per transfer frame, 129 B per
	// commit frame; 400 records ≈ 50 KiB, so a 32 KiB segment forces rotation
	// mid-batch.
	const nBatches, batchSize = 4, 100
	src, head := buildMultiSegmentWAL(t, nBatches, batchSize, 32<<10)

	entries, _ := os.ReadDir(src)
	if len(entries) < 2 {
		t.Fatalf("test setup did not rotate segments: %d segments", len(entries))
	}

	w, err := wal.Open(copyWALDir(t, src), 32<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	got, lsns := countTransfers(t, w, 0)
	if got != nBatches*batchSize {
		t.Fatalf("recovered %d transfer records, want %d — a rotation-straddling batch was dropped", got, nBatches*batchSize)
	}
	// Transfer LSNs interleave with commit-record LSNs (each batch's commit
	// takes the LSN right after its last record), so assert strict
	// monotonicity rather than consecutive values.
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Fatalf("LSN order broken at %d: %d not > %d", i, lsns[i], lsns[i-1])
		}
	}
	if w.CurrentLSN() != head {
		t.Fatalf("recovered watermark = %d, want head %d", w.CurrentLSN(), head)
	}

	// The stale-segment deletion path must not have fired: all segments still
	// present after recovery.
	after, _ := os.ReadDir(w.Dir())
	if len(after) != len(entries) {
		t.Fatalf("recovery deleted segments: had %d, now %d", len(entries), len(after))
	}
}

// C0.14: TruncateBefore deletes exactly the segments whose max LSN is below
// the given LSN, keeps the rest, and delta recovery from that LSN still
// succeeds. The cut LSN must be a committed-batch boundary (a snapshot LSN) —
// cutting mid-batch would orphan a straddling batch's records.
func TestTruncateBeforeDeletesOnlySegmentsBelowLSN(t *testing.T) {
	// ~24 × 6.3 KiB ≈ 150 KiB across 8 KiB segments → many segments. Each
	// segment holds exactly 66 LSNs (65×125 B records + one 29 B commit).
	const nBatches, batchSize = 24, 50
	src, head := buildMultiSegmentWAL(t, nBatches, batchSize, 8<<10)

	entries, _ := os.ReadDir(src)
	if len(entries) < 3 {
		t.Fatalf("test setup produced only %d segments; need several", len(entries))
	}

	// Batch k spans record LSNs (k-1)*51+1..k*51-1 with its commit at k*51.
	// LSN 102 is batch 2's commit — a genuine snapshot boundary. Segment 1
	// (max LSN 66) must be deleted; the rest kept.
	const cutLSN = 102

	w, err := wal.Open(copyWALDir(t, src), 8<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if err := w.TruncateBefore(cutLSN); err != nil {
		t.Fatalf("truncate before %d: %v", cutLSN, err)
	}

	after, _ := os.ReadDir(w.Dir())
	if len(after) >= len(entries) {
		t.Fatalf("expected some segments to be deleted, had %d now %d", len(entries), len(after))
	}

	// Delta recovery from the cut LSN replays exactly the batches above it:
	// batches 3..24 = 22 × 50 records, with the watermark at the true head.
	seen, _ := countTransfers(t, w, cutLSN)
	if seen != (nBatches-2)*batchSize {
		t.Fatalf("delta recovery after truncation saw %d transfers, want %d", seen, (nBatches-2)*batchSize)
	}
	if w.CurrentLSN() != head {
		t.Fatalf("watermark after truncation = %d, want %d", w.CurrentLSN(), head)
	}
}

// C0.13/C0.14: segments entirely at or below the resume LSN are not read at
// all during recovery. Total heap allocation across a multi-megabyte recovery
// must stay bounded by the tail chunk per skipped segment (~64 KiB) instead of
// the segment contents.
// C0.14: TruncateBefore must not read whole segments to find their max LSN —
// with 64 MiB production segments and a snapshot loop, the old full-read
// behaviour cost 64 MiB of I/O per segment per cycle forever. Heap allocation
// across a multi-megabyte truncate must stay bounded by one tail chunk per
// peeked segment (~64 KiB).
func TestTruncateBeforeDoesNotReadWholeSegments(t *testing.T) {
	// 12 batches × 8192 records × ~125 B ≈ 12 MiB across 1 MiB segments.
	const nBatches, batchSize = 12, 8192
	src, _ := buildMultiSegmentWAL(t, nBatches, batchSize, 1<<20)

	var totalSegmentBytes int64
	entries, _ := os.ReadDir(src)
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			totalSegmentBytes += fi.Size()
		}
	}
	if totalSegmentBytes < 8<<20 {
		t.Fatalf("test setup too small: %d segment bytes (< 8 MiB)", totalSegmentBytes)
	}

	w, err := wal.Open(copyWALDir(t, src), 1<<20)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	// Cut above the whole history's midpoint: the oldest segments get deleted
	// after a tail peek each, the rest get peeked and kept.
	if err := w.TruncateBefore(int64(nBatches*batchSize+3*nBatches) / 3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	runtime.ReadMemStats(&after)

	alloc := after.TotalAlloc - before.TotalAlloc
	// Old behaviour read every non-active segment fully (~totalSegmentBytes).
	// The bound allows one ~64 KiB tail chunk plus resync slack per peeked
	// segment, plus listing overhead.
	limit := uint64(len(entries))*(72<<10) + 256<<10
	if alloc > limit {
		t.Fatalf("TruncateBefore allocated %d bytes for %d bytes across %d segments (limit %d); whole segments are being read", alloc, totalSegmentBytes, len(entries), limit)
	}
}

func TestRecoverFromLSNDoesNotReadSegmentsBelowResumeLSN(t *testing.T) {
	// 64 batches * 512 records * ~125 B/frame ≈ 4 MiB across ~32 segments of
	// 128 KiB.
	const nBatches, batchSize = 64, 512
	src, head := buildMultiSegmentWAL(t, nBatches, batchSize, 128<<10)

	var totalSegmentBytes int64
	entries, _ := os.ReadDir(src)
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			totalSegmentBytes += fi.Size()
		}
	}
	if totalSegmentBytes < 2<<20 {
		t.Fatalf("test setup too small: %d segment bytes (< 2 MiB) cannot demonstrate the bound", totalSegmentBytes)
	}

	w, err := wal.Open(copyWALDir(t, src), 128<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	got, _ := countTransfers(t, w, head) // resume at head: every segment except the last must be skipped
	runtime.ReadMemStats(&after)

	if got != 0 {
		t.Fatalf("resume-at-head replayed %d transfer records, want 0", got)
	}
	alloc := after.TotalAlloc - before.TotalAlloc
	// Old behaviour read every segment fully (~totalSegmentBytes of heap). The
	// bound allows one ~64 KiB tail chunk per skipped segment plus a full read
	// and decode of the last segment plus slack.
	limit := uint64(len(entries))*(64<<10) + 768<<10
	if alloc > limit {
		t.Fatalf("recovery allocated %d bytes for %d bytes across %d segments (limit %d); segments below the resume LSN are being read", alloc, totalSegmentBytes, len(entries), limit)
	}
}
