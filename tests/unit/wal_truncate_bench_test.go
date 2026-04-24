package unit

import (
	"testing"

	"aequitas-ledger/internal/wal"
)

// BenchmarkWALTruncateBefore measures TruncateBefore over a WAL of many
// segments. C0.14: peekSegmentMaxLSN must read only each segment's tail, so
// B/op stays at ~one tail chunk per peeked segment instead of growing with
// total WAL size (the old implementation read every remaining segment fully —
// with 64 MiB production segments that is 64 MiB of I/O per segment per
// snapshot cycle).
//
// The cut LSN rises each iteration so each pass deletes a further slice of
// segments; B/op therefore reflects only TruncateBefore's own work (peek
// tails + deletes), with no per-iteration setup polluting the allocation
// count.
func BenchmarkWALTruncateBefore(b *testing.B) {
	const nBatches, batchSize = 128, 512 // ~8 MiB across 1 MiB segments
	dir, head := buildMultiSegmentWAL(b, nBatches, batchSize, 1<<20)

	w, err := wal.Open(dir, 64<<10)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer w.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		cut := head * int64(i) / int64(b.N+1)
		if err := w.TruncateBefore(cut); err != nil {
			b.Fatalf("truncate before %d: %v", cut, err)
		}
	}
}
