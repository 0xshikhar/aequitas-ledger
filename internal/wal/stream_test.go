package wal

import (
	"testing"
	"time"
)

// C0.8.4: a long-lived StreamReader must keep delivering after it has been
// caught up once. Regression test for a bug where the reader advanced past
// the live segment upon reaching its durable end and then never re-listed
// the same segment — permanently reporting caught-up while the WAL grew.
func TestStreamReaderLongLivedKeepsDelivering(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	// Batch 1 exists before the reader is created.
	if _, err := w.AppendBatch([]Record{
		{Type: RecordTypeAccount, Payload: make([]byte, 64)},
		{Type: RecordTypeAccount, Payload: make([]byte, 64)},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	sr := w.NewStreamReader(1)
	defer sr.Close()

	for i := 0; i < 3; i++ {
		if _, ok, err := sr.Next(); err != nil || !ok {
			t.Fatalf("step %d: ok=%v err=%v", i, ok, err)
		}
	}
	// Caught up at the durable end of the live segment.
	if _, ok, err := sr.Next(); err != nil || ok {
		t.Fatalf("expected caught-up, got ok=%v err=%v", ok, err)
	}

	// Batch 2 lands after the reader went caught-up: the same reader must
	// deliver all 50 records + the commit marker.
	recs := make([]Record, 50)
	for i := range recs {
		recs[i] = Record{Type: RecordTypeTransfer, Payload: make([]byte, 108)}
	}
	if _, err := w.AppendBatch(recs); err != nil {
		t.Fatalf("append batch 2: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync batch 2: %v", err)
	}

	got := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, ok, err := sr.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if !ok {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		got++
		if r.Type == RecordTypeBatchCommit {
			break
		}
	}
	if got != 51 {
		t.Fatalf("long-lived reader delivered %d/51 records of batch 2", got)
	}
}

// C0.8.4: the reader must never expose records beyond the synced bound —
// data written but not yet fsynced is invisible to the stream.
func TestStreamReaderHidesUnsyncedTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if _, err := w.AppendBatch([]Record{{Type: RecordTypeTransfer, Payload: make([]byte, 108)}}); err != nil {
		t.Fatalf("append (unsynced): %v", err)
	}
	// No Sync: the record is in the page cache only.

	sr := w.NewStreamReader(0)
	defer sr.Close()
	if _, ok, err := sr.Next(); err != nil || ok {
		t.Fatalf("unsynced record visible to stream: ok=%v err=%v", ok, err)
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	r, ok, err := sr.Next()
	if err != nil || !ok {
		t.Fatalf("synced record not visible: ok=%v err=%v", ok, err)
	}
	if r.Type != RecordTypeTransfer {
		t.Fatalf("got type %d, want transfer", r.Type)
	}
}

// C0.8.4: delta streaming across multiple segments — the reader starts at
// fromLSN (tail-peek skipping older segments) and delivers exactly the
// committed records above it, in order.
func TestStreamReaderDeltaAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 4<<10) // small segments → rotations
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	const batches, perBatch = 8, 20
	payload := make([]byte, 108)
	for b := 0; b < batches; b++ {
		recs := make([]Record, perBatch)
		for i := range recs {
			payload[0] = byte(b)
			recs[i] = Record{Type: RecordTypeTransfer, Payload: append([]byte(nil), payload...)}
		}
		if _, err := w.AppendBatch(recs); err != nil {
			t.Fatalf("append batch %d: %v", b, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Resume from the end of batch 4 (LSN 4*(20+1) = 84): batches 5..8.
	const fromLSN = 4 * (perBatch + 1)
	sr := w.NewStreamReader(fromLSN)
	defer sr.Close()

	seen := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, ok, err := sr.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if !ok {
			break // caught up
		}
		if r.Type == RecordTypeBatchCommit {
			continue // commits are delivered too; not counted as data
		}
		seen++
		if int64(r.LSN) <= fromLSN {
			t.Fatalf("delivered record %d at/below resume LSN %d", r.LSN, fromLSN)
		}
	}
	if seen != (batches-4)*perBatch {
		t.Fatalf("delta delivered %d records, want %d", seen, (batches-4)*perBatch)
	}
}
