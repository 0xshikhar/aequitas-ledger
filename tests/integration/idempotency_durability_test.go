package integration

import (
	"context"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestIdempotencyDurabilityAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	acc1ID := [16]byte{0x01}
	acc2ID := [16]byte{0x02}
	currency := [4]byte{'U', 'S', 'D', 0}

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

	_, _ = l1.CreateAccount(ctx, core.Account{ID: acc1ID, Currency: currency, PostedCredits: core.Uint128{Lo: 1000}})
	_, _ = l1.CreateAccount(ctx, core.Account{ID: acc2ID, Currency: currency})

	idempKey := [32]byte{0xAB, 0xCD, 0xEF}
	trID := [16]byte{0x55}

	// First execution with idempotency key
	tr1, err := l1.CreateTransfer(ctx, core.Transfer{
		ID:              trID,
		DebitAccountID:  acc1ID,
		CreditAccountID: acc2ID,
		Amount:          core.Uint128{Lo: 250},
		IdempotencyKey:  idempKey,
	})
	if err != nil {
		t.Fatalf("create transfer 1: %v", err)
	}

	if err := l1.Close(); err != nil {
		t.Fatalf("close ledger 1: %v", err)
	}

	// Reopen ledger from existing WAL (rebuilding idempotency index)
	w2, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}

	l2, err := engine.NewLedger(cfg, w2)
	if err != nil {
		t.Fatalf("new ledger 2: %v", err)
	}
	defer l2.Close()

	// Retry transfer with SAME idempotency key after restart
	tr2, err := l2.CreateTransfer(ctx, core.Transfer{
		ID:              [16]byte{0x99}, // different transaction ID
		DebitAccountID:  acc1ID,
		CreditAccountID: acc2ID,
		Amount:          core.Uint128{Lo: 250},
		IdempotencyKey:  idempKey,
	})
	if err != nil {
		t.Fatalf("retry transfer post-restart: %v", err)
	}

	// Should return ORIGINAL transfer result
	if tr2.ID != tr1.ID {
		t.Fatalf("idempotency result mismatch: got transfer ID %v want %v", tr2.ID, tr1.ID)
	}

	// Account 1 balance should STILL be 750 (debited ONCE, not twice)
	bal1, err := l2.GetBalance(ctx, acc1ID)
	if err != nil {
		t.Fatalf("get balance 1: %v", err)
	}
	if bal1.Lo != 750 {
		t.Fatalf("double debit occurred! acc1 balance got %d want 750", bal1.Lo)
	}
}
