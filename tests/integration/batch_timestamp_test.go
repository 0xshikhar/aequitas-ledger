package integration

import (
	"context"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestBatchSynchronousTimestampsAndMonotonicity(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w1, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open wal 1: %v", err)
	}
	l1, err := engine.NewLedger(engine.DefaultConfig(), w1)
	if err != nil {
		t.Fatalf("new ledger 1: %v", err)
	}

	ctx := context.Background()
	usd := [4]byte{'U', 'S', 'D', 0}
	acc1 := mkID(1)
	acc2 := mkID(2)

	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc1, Currency: usd, PostedCredits: core.Uint128{Lo: 10000}}); err != nil {
		t.Fatalf("create acc1: %v", err)
	}
	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc2, Currency: usd}); err != nil {
		t.Fatalf("create acc2: %v", err)
	}

	// Batch 1: 3 transfers submitted simultaneously
	batch1 := []core.Transfer{
		{ID: mkID(10), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10}},
		{ID: mkID(11), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 20}},
		{ID: mkID(12), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 30}},
	}

	outcomes1, err := l1.CreateTransfers(ctx, batch1)
	if err != nil {
		t.Fatalf("batch 1 error: %v", err)
	}
	for i, o := range outcomes1 {
		if o.Err != nil {
			t.Fatalf("batch 1 item %d error: %v", i, o.Err)
		}
	}

	ts1_0 := outcomes1[0].Transfer.Timestamp
	ts1_1 := outcomes1[1].Transfer.Timestamp
	ts1_2 := outcomes1[2].Transfer.Timestamp

	// Batch-synchronous timestamp discipline: all transfers in batch 1 must share the exact same timestamp
	if ts1_0 <= 0 {
		t.Fatalf("batch 1 timestamp %d must be > 0", ts1_0)
	}
	if ts1_0 != ts1_1 || ts1_1 != ts1_2 {
		t.Fatalf("batch 1 transfers have mismatched timestamps: %d, %d, %d", ts1_0, ts1_1, ts1_2)
	}

	// Batch 2: next transfer
	batch2 := []core.Transfer{
		{ID: mkID(20), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 15}},
	}
	outcomes2, err := l1.CreateTransfers(ctx, batch2)
	if err != nil {
		t.Fatalf("batch 2 error: %v", err)
	}
	ts2_0 := outcomes2[0].Transfer.Timestamp

	// Monotonicity: batch 2 timestamp must be strictly greater than batch 1
	if ts2_0 <= ts1_0 {
		t.Fatalf("batch 2 timestamp %d is not strictly greater than batch 1 timestamp %d", ts2_0, ts1_0)
	}

	// Close and reopen ledger to verify timestamps are replayed identically from WAL
	if err := l1.Close(); err != nil {
		t.Fatalf("close ledger 1: %v", err)
	}

	w2, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}
	l2, err := engine.NewLedger(engine.DefaultConfig(), w2)
	if err != nil {
		t.Fatalf("new ledger 2: %v", err)
	}
	defer l2.Close()

	rec10, err := l2.GetTransfer(ctx, mkID(10))
	if err != nil {
		t.Fatalf("get transfer 10: %v", err)
	}
	rec11, err := l2.GetTransfer(ctx, mkID(11))
	if err != nil {
		t.Fatalf("get transfer 11: %v", err)
	}
	rec20, err := l2.GetTransfer(ctx, mkID(20))
	if err != nil {
		t.Fatalf("get transfer 20: %v", err)
	}

	if rec10.Timestamp != ts1_0 {
		t.Errorf("recovered transfer 10 timestamp = %d, want %d", rec10.Timestamp, ts1_0)
	}
	if rec11.Timestamp != ts1_1 {
		t.Errorf("recovered transfer 11 timestamp = %d, want %d", rec11.Timestamp, ts1_1)
	}
	if rec20.Timestamp != ts2_0 {
		t.Errorf("recovered transfer 20 timestamp = %d, want %d", rec20.Timestamp, ts2_0)
	}
}
