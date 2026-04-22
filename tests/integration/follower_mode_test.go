package integration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestFollowerModeRejectsWrites(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	w, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	cfg := engine.DefaultConfig()
	cfg.IsFollower = true // Run as follower

	l, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ctx := context.Background()

	// Attempt CreateAccount on Follower
	_, err = l.CreateAccount(ctx, core.Account{ID: [16]byte{1}, Currency: [4]byte{'U', 'S', 'D'}})
	if err == nil || !errors.Is(err, core.ErrNotLeader{}) {
		t.Fatalf("expected ErrNotLeader on CreateAccount, got: %v", err)
	}

	// Attempt CreateTransfer on Follower
	_, err = l.CreateTransfer(ctx, core.Transfer{
		ID:              [16]byte{10},
		DebitAccountID:  [16]byte{1},
		CreditAccountID: [16]byte{2},
		Amount:          core.Uint128{Lo: 100},
	})
	if err == nil || !errors.Is(err, core.ErrNotLeader{}) {
		t.Fatalf("expected ErrNotLeader on CreateTransfer, got: %v", err)
	}
}
