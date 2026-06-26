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

// C0.7: the non-negative-balance invariant is enforced at every boundary by
// typed errors — never panics. An account whose posted debits exceed posted
// credits is rejected at creation, per item, and never reaches the WAL.
func TestInvariantBrokenAccountRejectedAtCreation(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	l, err := engine.NewLedger(engine.DefaultConfig(), w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	outcomes, err := l.CreateAccounts(ctx, []core.Account{
		{ID: mkID(1), Currency: [4]byte{'U', 'S', 'D', 0},
			PostedDebits:  core.Uint128{Lo: 100},
			PostedCredits: core.Uint128{Lo: 50}}, // invariant-broken
		{ID: mkID(2), Currency: [4]byte{'U', 'S', 'D', 0},
			PostedCredits: core.Uint128{Lo: 50}}, // valid
	})
	if err != nil {
		t.Fatalf("CreateAccounts: %v", err)
	}

	var inv core.ErrInvariantViolation
	if !errors.As(outcomes[0].Err, &inv) {
		t.Fatalf("item 0: want ErrInvariantViolation, got %v", outcomes[0].Err)
	}
	if outcomes[1].Err != nil {
		t.Fatalf("item 1 should succeed: %v", outcomes[1].Err)
	}

	// The rejected account must not exist.
	if _, err := l.GetAccount(ctx, mkID(1)); !errors.As(err, &core.ErrAccountNotFound{}) {
		t.Fatalf("invariant-broken account was created: %v", err)
	}
}

// C0.7: NewLedger rejects invariant-broken InitialAccounts at startup rather
// than writing them to the WAL.
func TestInvariantBrokenInitialAccountsRejected(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	cfg := engine.DefaultConfig()
	cfg.InitialAccounts = []core.Account{
		{ID: mkID(9), Currency: [4]byte{'U', 'S', 'D', 0},
			PostedDebits:  core.Uint128{Lo: 10},
			PostedCredits: core.Uint128{Lo: 1}},
	}
	if _, err := engine.NewLedger(cfg, w); err == nil {
		t.Fatal("NewLedger accepted an invariant-broken initial account")
	}
}
