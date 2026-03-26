package integration

import (
	"context"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func BenchmarkLedgerCreateTransferSingleGoroutine(b *testing.B) {
	dir := b.TempDir()
	w, err := wal.Open(dir, 64<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	cfg := engine.DefaultConfig()
	cfg.RingBufferSize = 1 << 16
	cfg.MaxBatchSize = 10_000
	cfg.BatchTimeout = time.Millisecond

	startingCredit, err := core.FromString("1000000000000000")
	if err != nil {
		b.Fatal(err)
	}
	debit := core.Account{ID: mkID(9001), Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: startingCredit}
	credit := core.Account{ID: mkID(9002), Currency: [4]byte{'U', 'S', 'D', 'C'}}
	cfg.InitialAccounts = []core.Account{debit, credit}

	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := mkID(uint64(i + 1))
		var key [32]byte
		copy(key[:16], id[:])
		_, err := ledger.CreateTransfer(ctx, core.Transfer{
			ID:              id,
			DebitAccountID:  debit.ID,
			CreditAccountID: credit.ID,
			Amount:          core.FromUint64(1),
			IdempotencyKey:  key,
		})
		if err != nil {
			b.Fatalf("create transfer failed at %d: %v", i, err)
		}
	}
}
