package unit

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/wal"
)

func TestWALAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	const n = 1000
	for i := 0; i < n; i++ {
		payload := makePayload(i)
		if _, err := w.AppendBatch([]wal.Record{{Type: wal.RecordTypeTransfer, Payload: payload}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync wal: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	reopened, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	seen := make([]int, 0, n)
	err = reopened.Recover(func(r wal.Record) error {
		if r.Type != wal.RecordTypeTransfer {
			return fmt.Errorf("unexpected type: %d", r.Type)
		}
		if len(r.Payload) != 8 {
			return fmt.Errorf("unexpected payload length: %d", len(r.Payload))
		}
		seen = append(seen, int(binary.BigEndian.Uint64(r.Payload)))
		return nil
	})
	if err != nil {
		t.Fatalf("recover wal: %v", err)
	}

	if len(seen) != n {
		t.Fatalf("recovered count mismatch: got=%d want=%d", len(seen), n)
	}
	for i := 0; i < n; i++ {
		if seen[i] != i {
			t.Fatalf("order mismatch at %d: got=%d want=%d", i, seen[i], i)
		}
	}
}

func TestWALRecoverTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	const n = 200
	for i := 0; i < n; i++ {
		payload := makePayload(i)
		if _, err := w.AppendBatch([]wal.Record{{Type: wal.RecordTypeTransfer, Payload: payload}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync wal: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	segments, err := filepath.Glob(filepath.Join(dir, "wal-*.seg"))
	if err != nil {
		t.Fatalf("glob segments: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("expected exactly one segment, got %d", len(segments))
	}
	seg := segments[0]

	st, err := os.Stat(seg)
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	if st.Size() <= 5 {
		t.Fatalf("segment unexpectedly too small: %d", st.Size())
	}

	// Simulate crash mid-write by truncating a few bytes from the tail.
	if err := os.Truncate(seg, st.Size()-5); err != nil {
		t.Fatalf("truncate segment: %v", err)
	}

	reopened, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	seen := make([]int, 0, n)
	err = reopened.Recover(func(r wal.Record) error {
		seen = append(seen, int(binary.BigEndian.Uint64(r.Payload)))
		return nil
	})
	if err != nil {
		t.Fatalf("recover wal: %v", err)
	}

	// Last record was incomplete and must be discarded.
	if len(seen) != n-1 {
		t.Fatalf("recovered count mismatch: got=%d want=%d", len(seen), n-1)
	}
	for i := 0; i < n-1; i++ {
		if seen[i] != i {
			t.Fatalf("order mismatch at %d: got=%d want=%d", i, seen[i], i)
		}
	}

	st2, err := os.Stat(seg)
	if err != nil {
		t.Fatalf("stat recovered segment: %v", err)
	}
	recordFrameLen := wal.RecordHeaderSize + 8 + wal.RecordCRCSize
	commitFrameLen := wal.RecordHeaderSize + 12 + wal.RecordCRCSize
	expectedSize := int64((n - 1) * (recordFrameLen + commitFrameLen))
	if st2.Size() != expectedSize {
		t.Fatalf("segment not truncated to last valid record: got=%d want=%d", st2.Size(), expectedSize)
	}
}

func makePayload(i int) []byte {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, uint64(i))
	return p
}
