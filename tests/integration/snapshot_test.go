package integration

import (
	"context"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestSnapshotRecoveryAndTruncation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	snapDir := filepath.Join(dir, "snapshots")

	// Use small segment size to force multiple segment rotations
	w1, err := wal.Open(walDir, 1<<14)
	if err != nil {
		t.Fatalf("open wal 1: %v", err)
	}

	cfg := engine.DefaultConfig()
	cfg.SnapshotDir = snapDir

	acc1ID := [16]byte{0x01}
	acc2ID := [16]byte{0x02}
	currency := [4]byte{'U', 'S', 'D', 0}

	l1, err := engine.NewLedger(cfg, w1)
	if err != nil {
		t.Fatalf("new ledger 1: %v", err)
	}

	ctx := context.Background()

	_, _ = l1.CreateAccount(ctx, core.Account{ID: acc1ID, Currency: currency, PostedCredits: core.Uint128{Lo: 10_000}})
	_, _ = l1.CreateAccount(ctx, core.Account{ID: acc2ID, Currency: currency})

	// Execute 100 transfers to generate WAL history across segments
	for i := 0; i < 100; i++ {
		var trID [16]byte
		trID[0] = byte(i + 1)
		var key [32]byte
		key[0] = byte(i + 1)
		_, err := l1.CreateTransfer(ctx, core.Transfer{
			ID:              trID,
			DebitAccountID:  acc1ID,
			CreditAccountID: acc2ID,
			Amount:          core.Uint128{Lo: 50},
			IdempotencyKey:  key,
		})
		if err != nil {
			t.Fatalf("transfer %d failed: %v", i, err)
		}
	}

	// Trigger snapshot at current LSN
	snapLSN, err := l1.TriggerSnapshot(ctx)
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	if snapLSN <= 0 {
		t.Fatalf("invalid snapshot LSN: %d", snapLSN)
	}

	// Close ledger 1
	if err := l1.Close(); err != nil {
		t.Fatalf("close ledger 1: %v", err)
	}

	// Reopen ledger 2 over same directory (loads snapshot + remaining WAL)
	w2, err := wal.Open(walDir, 1<<14)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}

	l2, err := engine.NewLedger(cfg, w2)
	if err != nil {
		t.Fatalf("new ledger 2: %v", err)
	}
	defer l2.Close()

	// Verify account balances post-snapshot recovery
	bal1, err := l2.GetBalance(ctx, acc1ID)
	if err != nil {
		t.Fatalf("get balance 1: %v", err)
	}
	if bal1.Lo != 5000 {
		t.Fatalf("acc1 balance got %d want 5000", bal1.Lo)
	}

	bal2, err := l2.GetBalance(ctx, acc2ID)
	if err != nil {
		t.Fatalf("get balance 2: %v", err)
	}
	if bal2.Lo != 5000 {
		t.Fatalf("acc2 balance got %d want 5000", bal2.Lo)
	}
}
