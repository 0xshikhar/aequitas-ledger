package unit

import (
	"context"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func setupAllocBenchLedger(b *testing.B) *engine.Ledger {
	dir := b.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	cfg := engine.DefaultConfig()
	l, err := engine.NewLedger(cfg, w)
	if err != nil {
		b.Fatalf("new ledger: %v", err)
	}
	b.Cleanup(func() { _ = l.Close() })

	ctx := context.Background()
	acc := core.Account{
		ID:            id16(1),
		Currency:      [4]byte{'U', 'S', 'D', 0},
		PostedCredits: core.Uint128{Lo: 1 << 62},
	}
	if _, err := l.CreateAccount(ctx, acc); err != nil {
		b.Fatalf("create account: %v", err)
	}
	acc2 := core.Account{ID: id16(2), Currency: [4]byte{'U', 'S', 'D', 0}, PostedCredits: core.Uint128{Lo: 1 << 62}}
	if _, err := l.CreateAccount(ctx, acc2); err != nil {
		b.Fatalf("create account 2: %v", err)
	}
	return l
}

// BenchmarkLedgerGetAccountAllocs measures the synchronous read path.
// With pooled one-shot result channels (C0.15) this must report 0 allocs/op.
func BenchmarkLedgerGetAccountAllocs(b *testing.B) {
	l := setupAllocBenchLedger(b)
	ctx := context.Background()
	id := id16(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.GetAccount(ctx, id); err != nil {
			b.Fatalf("get account: %v", err)
		}
	}
}

// BenchmarkLedgerCreateTransferAllocs measures the submit path end to end
// (idempotency reserve, ring-buffer submit, WAL encode/append, fsync, apply,
// ack). The pooled result channel removes the per-transfer channel allocation
// (C0.15); the remaining allocations are WAL record encoding (addressed
// separately in S2.1).
func BenchmarkLedgerCreateTransferAllocs(b *testing.B) {
	l := setupAllocBenchLedger(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var trID [16]byte
		var key [32]byte
		putUint64(trID[:8], uint64(i+1))
		putUint64(key[:8], uint64(i+1)) // unique idempotency key, else every op after the first is a dedup cache hit
		tr := core.Transfer{
			ID:              trID,
			DebitAccountID:  id16(1),
			CreditAccountID: id16(2),
			Amount:          core.Uint128{Lo: 1},
			IdempotencyKey:  key,
		}
		if _, err := l.CreateTransfer(ctx, tr); err != nil {
			b.Fatalf("create transfer: %v", err)
		}
	}
}

func putUint64(dst []byte, v uint64) {
	for i := 7; i >= 0; i-- {
		dst[i] = byte(v)
		v >>= 8
	}
}
