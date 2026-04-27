package unit

import (
	"fmt"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/wal"
)

func TestAlignedBuffer(t *testing.T) {
	for _, size := range []int{1, 100, 4096, 8192, 10000} {
		buf := wal.NewAlignedBuffer(size, wal.DirectIOBlockSize)
		if len(buf) != size {
			t.Fatalf("expected len %d, got %d", size, len(buf))
		}
		if !wal.IsAligned(buf, wal.DirectIOBlockSize) {
			t.Fatalf("buffer not aligned to %d bytes", wal.DirectIOBlockSize)
		}
	}
}

func TestSegmentPreallocationAndDirectIO(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal-prealloc")

	// Open WAL with DirectIO requested (uses O_DIRECT on Linux with fallback on dev/macOS)
	w, err := wal.OpenWithOptions(dir, 64<<20, true)
	if err != nil {
		t.Fatalf("failed to open wal with direct IO: %v", err)
	}

	// Append batches of varying sizes: small, multi-KB, crossing block boundaries
	recordCounts := []int{1, 5, 20, 50}
	totalAppended := 0
	for batchIdx, count := range recordCounts {
		records := make([]wal.Record, count)
		for i := 0; i < count; i++ {
			records[i] = wal.Record{
				Type:    wal.RecordTypeTransfer,
				Payload: []byte(fmt.Sprintf("batch-%d-record-%d-%0100d", batchIdx, i, i)),
			}
			totalAppended++
		}
		if _, err := w.AppendBatch(records); err != nil {
			t.Fatalf("AppendBatch %d failed: %v", batchIdx, err)
		}
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and recover
	w2, err := wal.OpenWithOptions(dir, 64<<20, false)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}
	defer w2.Close()

	recoveredCount := 0
	err = w2.Recover(func(r wal.Record) error {
		if r.Type == wal.RecordTypeTransfer {
			recoveredCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	if recoveredCount != totalAppended {
		t.Fatalf("expected %d recovered records, got %d", totalAppended, recoveredCount)
	}
}
