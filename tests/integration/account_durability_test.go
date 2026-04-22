package integration

import (
	"context"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestAccountDurabilityAcrossCrash(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	acc1ID := [16]byte{0x0A}
	acc2ID := [16]byte{0x0B}
	currency := [4]byte{'U', 'S', 'D', 0}

	// 1. First run: Start ledger with empty accounts
	w1, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open wal 1: %v", err)
	}

	cfg := engine.DefaultConfig()
	l1, err := engine.NewLedger(cfg, w1)
	if err != nil {
		t.Fatalf("new ledger 1: %v", err)
	}

	ctx := context.Background()

	// Create account 1 via API
	_, err = l1.CreateAccount(ctx, core.Account{
		ID:            acc1ID,
		Currency:      currency,
		PostedCredits: core.Uint128{Lo: 500},
	})
	if err != nil {
		t.Fatalf("create account 1: %v", err)
	}

	// Create account 2 via API
	_, err = l1.CreateAccount(ctx, core.Account{
		ID:       acc2ID,
		Currency: currency,
	})
	if err != nil {
		t.Fatalf("create account 2: %v", err)
	}

	// Perform a transfer between acc1 and acc2
	trID := [16]byte{0x99}
	_, err = l1.CreateTransfer(ctx, core.Transfer{
		ID:              trID,
		DebitAccountID:  acc1ID,
		CreditAccountID: acc2ID,
		Amount:          core.Uint128{Lo: 100},
	})
	if err != nil {
		t.Fatalf("create transfer: %v", err)
	}

	// Close ledger (simulating shutdown/restart)
	if err := l1.Close(); err != nil {
		t.Fatalf("close ledger 1: %v", err)
	}

	// 2. Second run: Reopen ledger from existing WAL directory (NO InitialAccounts seeding!)
	w2, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}

	l2, err := engine.NewLedger(cfg, w2)
	if err != nil {
		t.Fatalf("new ledger 2: %v", err)
	}
	defer l2.Close()

	// Assert Account 1 exists and has correct post-transfer balance
	acc1, err := l2.GetAccount(ctx, acc1ID)
	if err != nil {
		t.Fatalf("get account 1 post recovery: %v", err)
	}
	if acc1.Currency != currency {
		t.Fatalf("currency mismatch: got %v want %v", acc1.Currency, currency)
	}

	bal1, err := l2.GetBalance(ctx, acc1ID)
	if err != nil {
		t.Fatalf("get balance 1: %v", err)
	}
	if bal1.Lo != 400 {
		t.Fatalf("acc1 balance mismatch: got %d want 400", bal1.Lo)
	}

	// Assert Account 2 exists and has correct post-transfer balance
	bal2, err := l2.GetBalance(ctx, acc2ID)
	if err != nil {
		t.Fatalf("get balance 2: %v", err)
	}
	if bal2.Lo != 100 {
		t.Fatalf("acc2 balance mismatch: got %d want 100", bal2.Lo)
	}
}
