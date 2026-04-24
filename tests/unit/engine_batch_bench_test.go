package unit

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"aequitas-ledger/internal/core"
)

// BenchmarkLedgerCreateTransfersBatch measures the batched submit path (D1.1)
// at several batch sizes. One CreateTransfers call = one client batch: N
// ring-buffer submissions, one shared completion, one fsync amortized over N
// transfers. Compare against BenchmarkLedgerCreateTransferAllocs (unary):
// allocs/op here is per CALL (≈7 allocations regardless of batch size), so
// per-transfer allocation cost approaches zero.
func BenchmarkLedgerCreateTransfersBatch(b *testing.B) {
	for _, size := range []int{1, 128, 1024, 8192} {
		b.Run(fmt.Sprintf("batch-%d", size), func(b *testing.B) {
			l := setupAllocBenchLedger(b)
			ctx := context.Background()

			var counter atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := make([]core.Transfer, size)
				base := counter.Add(uint64(size))
				for k := range batch {
					n := base - uint64(size) + uint64(k) + 1
					var trID [16]byte
					var key [32]byte
					putUint64(trID[:8], n) // unique per transfer: no dedup hits
					putUint64(key[:8], n)
					batch[k] = core.Transfer{
						ID:              trID,
						DebitAccountID:  id16(1),
						CreditAccountID: id16(2),
						Amount:          core.Uint128{Lo: 1},
						IdempotencyKey:  key,
					}
				}
				outs, err := l.CreateTransfers(ctx, batch)
				if err != nil {
					b.Fatalf("CreateTransfers: %v", err)
				}
				for j := range outs {
					if outs[j].Err != nil {
						b.Fatalf("item %d failed: %v", j, outs[j].Err)
					}
				}
			}
		})
	}
}
