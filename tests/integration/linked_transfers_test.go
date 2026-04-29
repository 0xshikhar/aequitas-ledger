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

func TestLinkedTransfersMultiLegSuccess(t *testing.T) {
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
	usd := [4]byte{'U', 'S', 'D', 0}

	accA := mkID(1)
	accEscrow := mkID(2)
	accB := mkID(3)

	if _, err := l.CreateAccount(ctx, core.Account{ID: accA, Currency: usd, PostedCredits: core.Uint128{Lo: 1000}}); err != nil {
		t.Fatalf("create A: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: accEscrow, Currency: usd}); err != nil {
		t.Fatalf("create Escrow: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: accB, Currency: usd}); err != nil {
		t.Fatalf("create B: %v", err)
	}

	// Multi-leg chain: A -> Escrow (linked), Escrow -> B (unlinked)
	// Escrow has 0 balance originally, so Escrow -> B can only succeed if it observes the staged
	// balance from Leg 1!
	tr1 := core.Transfer{
		ID:              mkID(101),
		DebitAccountID:  accA,
		CreditAccountID: accEscrow,
		Amount:          core.Uint128{Lo: 300},
		Flags:           core.TransferFlagLinked,
	}
	tr2 := core.Transfer{
		ID:              mkID(102),
		DebitAccountID:  accEscrow,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 300},
		Flags:           0, // terminates the chain
	}

	outcomes, err := l.CreateTransfers(ctx, []core.Transfer{tr1, tr2})
	if err != nil {
		t.Fatalf("CreateTransfers error: %v", err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("expected 2 outcomes, got %d", len(outcomes))
	}
	if outcomes[0].Err != nil {
		t.Errorf("leg 1 failed: %v", outcomes[0].Err)
	}
	if outcomes[1].Err != nil {
		t.Errorf("leg 2 failed: %v", outcomes[1].Err)
	}

	balA, _ := l.GetBalance(ctx, accA)
	balEscrow, _ := l.GetBalance(ctx, accEscrow)
	balB, _ := l.GetBalance(ctx, accB)

	if balA.Lo != 700 {
		t.Errorf("A balance = %d, want 700", balA.Lo)
	}
	if balEscrow.Lo != 0 {
		t.Errorf("Escrow balance = %d, want 0", balEscrow.Lo)
	}
	if balB.Lo != 300 {
		t.Errorf("B balance = %d, want 300", balB.Lo)
	}
}

func TestLinkedTransfersFailureRollsBackAllLegs(t *testing.T) {
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
	usd := [4]byte{'U', 'S', 'D', 0}

	accA := mkID(1)
	accEscrow := mkID(2)
	accB := mkID(3)

	if _, err := l.CreateAccount(ctx, core.Account{ID: accA, Currency: usd, PostedCredits: core.Uint128{Lo: 1000}}); err != nil {
		t.Fatalf("create A: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: accEscrow, Currency: usd}); err != nil {
		t.Fatalf("create Escrow: %v", err)
	}
	// B is frozen!
	if _, err := l.CreateAccount(ctx, core.Account{ID: accB, Currency: usd, Flags: core.AccountFlagFrozen}); err != nil {
		t.Fatalf("create B: %v", err)
	}

	startLSN := l.CurrentLSN()

	// Leg 1: A -> Escrow (valid, linked)
	// Leg 2: Escrow -> B (fails because B is frozen)
	tr1 := core.Transfer{
		ID:              mkID(201),
		DebitAccountID:  accA,
		CreditAccountID: accEscrow,
		Amount:          core.Uint128{Lo: 500},
		Flags:           core.TransferFlagLinked,
	}
	tr2 := core.Transfer{
		ID:              mkID(202),
		DebitAccountID:  accEscrow,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 500},
		Flags:           0,
	}

	outcomes, err := l.CreateTransfers(ctx, []core.Transfer{tr1, tr2})
	if err != nil {
		t.Fatalf("CreateTransfers error: %v", err)
	}

	var chainFailed core.ErrLinkedChainFailed
	if !errors.As(outcomes[0].Err, &chainFailed) {
		t.Errorf("leg 1 expected ErrLinkedChainFailed, got %v", outcomes[0].Err)
	}

	var accFrozen core.ErrAccountFrozen
	if !errors.As(outcomes[1].Err, &accFrozen) {
		t.Errorf("leg 2 expected ErrAccountFrozen, got %v", outcomes[1].Err)
	}

	// Zero WAL records must have been committed for the failed chain
	if l.CurrentLSN() != startLSN {
		t.Errorf("LSN advanced from %d to %d despite chain rollback", startLSN, l.CurrentLSN())
	}

	// Balances must remain completely untouched
	balA, _ := l.GetBalance(ctx, accA)
	balEscrow, _ := l.GetBalance(ctx, accEscrow)
	if balA.Lo != 1000 {
		t.Errorf("A balance = %d, want 1000", balA.Lo)
	}
	if balEscrow.Lo != 0 {
		t.Errorf("Escrow balance = %d, want 0", balEscrow.Lo)
	}
}

func TestLinkedTransfersDanglingOpenChain(t *testing.T) {
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
	usd := [4]byte{'U', 'S', 'D', 0}

	accA := mkID(1)
	accB := mkID(2)

	if _, err := l.CreateAccount(ctx, core.Account{ID: accA, Currency: usd, PostedCredits: core.Uint128{Lo: 1000}}); err != nil {
		t.Fatalf("create A: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: accB, Currency: usd}); err != nil {
		t.Fatalf("create B: %v", err)
	}

	// Single transfer with flag_linked set at the end of the batch
	tr := core.Transfer{
		ID:              mkID(301),
		DebitAccountID:  accA,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 100},
		Flags:           core.TransferFlagLinked,
	}

	outcomes, err := l.CreateTransfers(ctx, []core.Transfer{tr})
	if err != nil {
		t.Fatalf("CreateTransfers: %v", err)
	}
	var chainOpen core.ErrLinkedChainOpen
	if !errors.As(outcomes[0].Err, &chainOpen) {
		t.Errorf("expected ErrLinkedChainOpen, got %v", outcomes[0].Err)
	}

	balA, _ := l.GetBalance(ctx, accA)
	if balA.Lo != 1000 {
		t.Errorf("A balance = %d, want 1000", balA.Lo)
	}
}

func TestMixedBatchIsolation(t *testing.T) {
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
	usd := [4]byte{'U', 'S', 'D', 0}

	accA := mkID(1)
	accB := mkID(2)
	accC := mkID(3)

	if _, err := l.CreateAccount(ctx, core.Account{ID: accA, Currency: usd, PostedCredits: core.Uint128{Lo: 1000}}); err != nil {
		t.Fatalf("create A: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: accB, Currency: usd, PostedCredits: core.Uint128{Lo: 1000}}); err != nil {
		t.Fatalf("create B: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: accC, Currency: usd, PostedCredits: core.Uint128{Lo: 1000}}); err != nil {
		t.Fatalf("create C: %v", err)
	}

	// Item 0: Independent transfer (A -> B: 50) -> OK
	// Items 1-2: Linked chain (B -> C: 100 [linked], C -> A: 5000 [unlinked, insufficient funds]) -> FAIL
	// Item 3: Independent transfer (C -> B: 30) -> OK
	batch := []core.Transfer{
		{ID: mkID(401), DebitAccountID: accA, CreditAccountID: accB, Amount: core.Uint128{Lo: 50}},
		{ID: mkID(402), DebitAccountID: accB, CreditAccountID: accC, Amount: core.Uint128{Lo: 100}, Flags: core.TransferFlagLinked},
		{ID: mkID(403), DebitAccountID: accC, CreditAccountID: accA, Amount: core.Uint128{Lo: 5000}, Flags: 0},
		{ID: mkID(404), DebitAccountID: accC, CreditAccountID: accB, Amount: core.Uint128{Lo: 30}},
	}

	outcomes, err := l.CreateTransfers(ctx, batch)
	if err != nil {
		t.Fatalf("CreateTransfers: %v", err)
	}

	if outcomes[0].Err != nil {
		t.Errorf("item 0 failed: %v", outcomes[0].Err)
	}
	var chainFailed core.ErrLinkedChainFailed
	if !errors.As(outcomes[1].Err, &chainFailed) {
		t.Errorf("item 1 expected ErrLinkedChainFailed, got %v", outcomes[1].Err)
	}
	var insufficient core.ErrInsufficientFunds
	if !errors.As(outcomes[2].Err, &insufficient) {
		t.Errorf("item 2 expected ErrInsufficientFunds, got %v", outcomes[2].Err)
	}
	if outcomes[3].Err != nil {
		t.Errorf("item 3 failed: %v", outcomes[3].Err)
	}

	balA, _ := l.GetBalance(ctx, accA)
	balB, _ := l.GetBalance(ctx, accB)
	balC, _ := l.GetBalance(ctx, accC)

	// A started with 1000, gave 50 to B -> 950
	if balA.Lo != 950 {
		t.Errorf("A balance = %d, want 950", balA.Lo)
	}
	// B started with 1000, got 50 from A, got 30 from C -> 1080
	if balB.Lo != 1080 {
		t.Errorf("B balance = %d, want 1080", balB.Lo)
	}
	// C started with 1000, gave 30 to B -> 970
	if balC.Lo != 970 {
		t.Errorf("C balance = %d, want 970", balC.Lo)
	}
}
