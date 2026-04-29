package integration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/audit"
	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestMultiTenantLedgerIsolation(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := wal.Open(walDir, 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	l, err := engine.NewLedger(engine.DefaultConfig(), w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	usd := [4]byte{'U', 'S', 'D', 0}

	// Ledger 100: Organization Alpha
	accAlpha1 := mkID(1)
	accAlpha2 := mkID(2)
	// Ledger 200: Organization Beta
	accBeta1 := mkID(3)
	accBeta2 := mkID(4)

	if _, err := l.CreateAccount(ctx, core.Account{
		ID: accAlpha1, Currency: usd, Ledger: 100, Code: 10, PostedCredits: core.Uint128{Lo: 5000},
	}); err != nil {
		t.Fatalf("create alpha1: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{
		ID: accAlpha2, Currency: usd, Ledger: 100, Code: 20,
	}); err != nil {
		t.Fatalf("create alpha2: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{
		ID: accBeta1, Currency: usd, Ledger: 200, Code: 10, PostedCredits: core.Uint128{Lo: 8000},
	}); err != nil {
		t.Fatalf("create beta1: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{
		ID: accBeta2, Currency: usd, Ledger: 200, Code: 20,
	}); err != nil {
		t.Fatalf("create beta2: %v", err)
	}

	// 1. Cross-tenant transfer attempt (Alpha1 -> Beta1) must fail with ErrLedgerMismatch
	crossTr := core.Transfer{
		ID:              mkID(10),
		DebitAccountID:  accAlpha1,
		CreditAccountID: accBeta1,
		Amount:          core.Uint128{Lo: 100},
		Ledger:          100,
	}
	_, err = l.CreateTransfer(ctx, crossTr)
	var ledgerMismatch core.ErrLedgerMismatch
	if !errors.As(err, &ledgerMismatch) {
		t.Fatalf("expected ErrLedgerMismatch, got %v", err)
	}

	// 2. Transfer with mismatching transfer.Ledger must fail
	badLedgerTr := core.Transfer{
		ID:              mkID(11),
		DebitAccountID:  accAlpha1,
		CreditAccountID: accAlpha2,
		Amount:          core.Uint128{Lo: 100},
		Ledger:          200, // Accounts are in Ledger 100!
	}
	_, err = l.CreateTransfer(ctx, badLedgerTr)
	if !errors.As(err, &ledgerMismatch) {
		t.Fatalf("expected ErrLedgerMismatch on bad transfer.Ledger, got %v", err)
	}

	// 3. Valid intra-ledger transfers for both tenants
	trAlpha := core.Transfer{
		ID:              mkID(12),
		DebitAccountID:  accAlpha1,
		CreditAccountID: accAlpha2,
		Amount:          core.Uint128{Lo: 1500},
		Ledger:          100,
		Code:            101,
		UserData128:     [16]byte{1, 2, 3},
	}
	trBeta := core.Transfer{
		ID:              mkID(13),
		DebitAccountID:  accBeta1,
		CreditAccountID: accBeta2,
		Amount:          core.Uint128{Lo: 3200},
		Ledger:          200,
		Code:            202,
		UserData128:     [16]byte{4, 5, 6},
	}

	if _, err := l.CreateTransfer(ctx, trAlpha); err != nil {
		t.Fatalf("create trAlpha: %v", err)
	}
	if _, err := l.CreateTransfer(ctx, trBeta); err != nil {
		t.Fatalf("create trBeta: %v", err)
	}

	// Verify balances
	balA1, _ := l.GetBalance(ctx, accAlpha1)
	balA2, _ := l.GetBalance(ctx, accAlpha2)
	if balA1.Lo != 3500 || balA2.Lo != 1500 {
		t.Errorf("Alpha balances unexpected: A1=%d, A2=%d", balA1.Lo, balA2.Lo)
	}

	balB1, _ := l.GetBalance(ctx, accBeta1)
	balB2, _ := l.GetBalance(ctx, accBeta2)
	if balB1.Lo != 4800 || balB2.Lo != 3200 {
		t.Errorf("Beta balances unexpected: B1=%d, B2=%d", balB1.Lo, balB2.Lo)
	}

	// 4. Verify audit tool passes per-ledger balance invariant check
	rep, err := audit.Audit(walDir)
	if err != nil {
		t.Fatalf("audit failed: %v", err)
	}
	if !rep.Valid() {
		t.Fatalf("audit reported violations: %+v", rep.Violations)
	}
}
