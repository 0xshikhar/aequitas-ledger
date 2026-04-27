package integration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

// D1.2 test: two-phase transfer (hold) full lifecycle — reservation,
// partial post settlement, full settlement, and invariant verification.
func TestTwoPhaseHoldLifecycle(t *testing.T) {
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
	accA := mkID(1)
	accB := mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}

	if _, err := l.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 1000}},
		{ID: accB, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}

	// 1. Open hold for 400
	holdID := mkID(101)
	outcomes, err := l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:              holdID,
			DebitAccountID:  accA,
			CreditAccountID: accB,
			Amount:          core.Uint128{Lo: 400},
			Flags:           core.TransferFlagPending,
		},
	})
	if err != nil || outcomes[0].Err != nil {
		t.Fatalf("open hold: %v", outcomes[0].Err)
	}

	// Check Account A balances
	a, err := l.GetAccount(ctx, accA)
	if err != nil {
		t.Fatalf("get accA: %v", err)
	}
	if a.PostedDebits.Lo != 0 || a.PendingDebits.Lo != 400 {
		t.Fatalf("accA debits: posted=%d pending=%d, want 0/400", a.PostedDebits.Lo, a.PendingDebits.Lo)
	}
	availA, _ := core.AvailableBalance(a)
	if availA.Lo != 600 {
		t.Fatalf("accA available = %d, want 600", availA.Lo)
	}

	// 2. Try reserving 700: should fail because available balance is 600
	outcomes, err = l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:              mkID(102),
			DebitAccountID:  accA,
			CreditAccountID: accB,
			Amount:          core.Uint128{Lo: 700},
			Flags:           core.TransferFlagPending,
		},
	})
	if err != nil {
		t.Fatalf("batch error: %v", err)
	}
	var insuff core.ErrInsufficientFunds
	if !errors.As(outcomes[0].Err, &insuff) {
		t.Fatalf("expected ErrInsufficientFunds, got %v", outcomes[0].Err)
	}

	// 3. Partial post: settle 250 out of 400
	outcomes, err = l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:    holdID,
			Flags: core.TransferFlagPostPending,
			Amount: core.Uint128{Lo: 250},
		},
	})
	if err != nil || outcomes[0].Err != nil {
		t.Fatalf("partial post: %v", outcomes[0].Err)
	}

	a, _ = l.GetAccount(ctx, accA)
	if a.PostedDebits.Lo != 250 || a.PendingDebits.Lo != 150 {
		t.Fatalf("accA debits after partial post: posted=%d pending=%d, want 250/150", a.PostedDebits.Lo, a.PendingDebits.Lo)
	}
	b, _ := l.GetAccount(ctx, accB)
	if b.PostedCredits.Lo != 250 || b.PendingCredits.Lo != 150 {
		t.Fatalf("accB credits after partial post: posted=%d pending=%d, want 250/150", b.PostedCredits.Lo, b.PendingCredits.Lo)
	}

	// 4. Over-transfer: trying to post more than remaining hold (200 > 150)
	outcomes, err = l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:     holdID,
			Flags:  core.TransferFlagPostPending,
			Amount: core.Uint128{Lo: 200},
		},
	})
	if err != nil {
		t.Fatalf("batch error: %v", err)
	}
	var overTransfer core.ErrOverTransfer
	if !errors.As(outcomes[0].Err, &overTransfer) {
		t.Fatalf("expected ErrOverTransfer, got %v", outcomes[0].Err)
	}

	// 5. Full settle remainder: Amount = 0 settles remaining 150
	outcomes, err = l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:     holdID,
			Flags:  core.TransferFlagPostPending,
			Amount: core.Uint128{},
		},
	})
	if err != nil || outcomes[0].Err != nil {
		t.Fatalf("settle remaining: %v", outcomes[0].Err)
	}

	a, _ = l.GetAccount(ctx, accA)
	b, _ = l.GetAccount(ctx, accB)
	if a.PostedDebits.Lo != 400 || a.PendingDebits.Lo != 0 {
		t.Fatalf("accA final: posted=%d pending=%d, want 400/0", a.PostedDebits.Lo, a.PendingDebits.Lo)
	}
	if b.PostedCredits.Lo != 400 || b.PendingCredits.Lo != 0 {
		t.Fatalf("accB final: posted=%d pending=%d, want 400/0", b.PostedCredits.Lo, b.PendingCredits.Lo)
	}
}

// D1.2 test: hold voiding releases reserved funds.
func TestTwoPhaseHoldVoid(t *testing.T) {
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
	accA := mkID(1)
	accB := mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}

	if _, err := l.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 1000}},
		{ID: accB, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}

	holdID := mkID(201)
	outcomes, err := l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:              holdID,
			DebitAccountID:  accA,
			CreditAccountID: accB,
			Amount:          core.Uint128{Lo: 500},
			Flags:           core.TransferFlagPending,
		},
	})
	if err != nil || outcomes[0].Err != nil {
		t.Fatalf("create hold: %v", outcomes[0].Err)
	}

	// Void the hold
	outcomes, err = l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:    holdID,
			Flags: core.TransferFlagVoidPending,
		},
	})
	if err != nil || outcomes[0].Err != nil {
		t.Fatalf("void hold: %v", outcomes[0].Err)
	}

	a, _ := l.GetAccount(ctx, accA)
	if a.PendingDebits.Lo != 0 || a.PostedDebits.Lo != 0 {
		t.Fatalf("accA pending debits = %d, want 0", a.PendingDebits.Lo)
	}
	availA, _ := core.AvailableBalance(a)
	if availA.Lo != 1000 {
		t.Fatalf("accA available balance = %d, want 1000", availA.Lo)
	}

	// Double void must fail with ErrPendingNotFound
	outcomes, _ = l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:    holdID,
			Flags: core.TransferFlagVoidPending,
		},
	})
	var notFound core.ErrPendingNotFound
	if !errors.As(outcomes[0].Err, &notFound) {
		t.Fatalf("expected ErrPendingNotFound on double void, got %v", outcomes[0].Err)
	}
}

// D1.2 test: hold timeout expiry is judged by the logical clock.
func TestTwoPhaseHoldExpiryOnLogicalClock(t *testing.T) {
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
	accA := mkID(1)
	accB := mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}

	if _, err := l.CreateAccounts(ctx, []core.Account{
		{ID: accA, Currency: cur, PostedCredits: core.Uint128{Lo: 1000}},
		{ID: accB, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}

	holdID := mkID(301)
	// Timeout 1 millisecond (1_000_000 ns)
	outcomes, err := l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:              holdID,
			DebitAccountID:  accA,
			CreditAccountID: accB,
			Amount:          core.Uint128{Lo: 350},
			Flags:           core.TransferFlagPending,
			Timeout:         1_000_000,
		},
	})
	if err != nil || outcomes[0].Err != nil {
		t.Fatalf("create hold: %v", outcomes[0].Err)
	}

	// Sleep 5ms to guarantee wall clock and next batch timestamp advances past expiry
	time.Sleep(5 * time.Millisecond)

	// Trigger next batch by sending a transfer
	_, err = l.CreateTransfer(ctx, core.Transfer{
		ID:              mkID(302),
		DebitAccountID:  accA,
		CreditAccountID: accB,
		Amount:          core.Uint128{Lo: 10},
	})
	if err != nil {
		t.Fatalf("trigger transfer: %v", err)
	}

	// The hold should have expired and voided automatically
	a, _ := l.GetAccount(ctx, accA)
	if a.PendingDebits.Lo != 0 {
		t.Fatalf("accA pending debits = %d, want 0 after expiry", a.PendingDebits.Lo)
	}
	availA, _ := core.AvailableBalance(a)
	if availA.Lo != 990 { // 1000 - 10 posted
		t.Fatalf("accA available balance = %d, want 990", availA.Lo)
	}

	// Attempting to settle expired hold should fail with ErrPendingNotFound
	outcomes, _ = l.CreateTransfers(ctx, []core.Transfer{
		{
			ID:    holdID,
			Flags: core.TransferFlagPostPending,
		},
	})
	var notFoundExp core.ErrPendingNotFound
	if !errors.As(outcomes[0].Err, &notFoundExp) {
		t.Fatalf("expected ErrPendingNotFound for expired hold, got %v", outcomes[0].Err)
	}
}

// D1.5 test: Query API — point lookups and cursor-paginated transfer history.
func TestQueryAPITransfersAndHistory(t *testing.T) {
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
	acc1 := mkID(1)
	acc2 := mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}

	if _, err := l.CreateAccounts(ctx, []core.Account{
		{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 100_000}},
		{ID: acc2, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}

	const nTransfers = 10
	for i := 1; i <= nTransfers; i++ {
		_, err := l.CreateTransfer(ctx, core.Transfer{
			ID:              mkID(uint64(100 + i)),
			DebitAccountID:  acc1,
			CreditAccountID: acc2,
			Amount:          core.Uint128{Lo: uint64(i * 10)},
		})
		if err != nil {
			t.Fatalf("create transfer %d: %v", i, err)
		}
	}

	// 1. Point lookup: existing transfer
	tr, err := l.GetTransfer(ctx, mkID(105))
	if err != nil {
		t.Fatalf("get transfer 105: %v", err)
	}
	if tr.Amount.Lo != 50 {
		t.Fatalf("transfer 105 amount = %d, want 50", tr.Amount.Lo)
	}
	if tr.Timestamp == 0 {
		t.Fatalf("transfer 105 timestamp is 0")
	}

	// 2. Point lookup: non-existent transfer
	_, err = l.GetTransfer(ctx, mkID(999))
	var notFound core.ErrTransferNotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("expected ErrTransferNotFound, got %v", err)
	}

	// 3. Paginated list: page 1 of 4 items
	page1, err := l.GetAccountTransfers(ctx, acc1, [16]byte{}, 4)
	if err != nil {
		t.Fatalf("get page 1: %v", err)
	}
	if len(page1) != 4 {
		t.Fatalf("page 1 len = %d, want 4", len(page1))
	}
	if page1[0].ID != mkID(101) || page1[3].ID != mkID(104) {
		t.Fatalf("page 1 IDs mismatch: first=%x, last=%x", page1[0].ID, page1[3].ID)
	}

	// 4. Page 2: after page1[3].ID
	page2, err := l.GetAccountTransfers(ctx, acc1, page1[3].ID, 4)
	if err != nil {
		t.Fatalf("get page 2: %v", err)
	}
	if len(page2) != 4 {
		t.Fatalf("page 2 len = %d, want 4", len(page2))
	}
	if page2[0].ID != mkID(105) || page2[3].ID != mkID(108) {
		t.Fatalf("page 2 IDs mismatch: first=%x, last=%x", page2[0].ID, page2[3].ID)
	}

	// 5. Page 3: remainder
	page3, err := l.GetAccountTransfers(ctx, acc1, page2[3].ID, 4)
	if err != nil {
		t.Fatalf("get page 3: %v", err)
	}
	if len(page3) != 2 {
		t.Fatalf("page 3 len = %d, want 2", len(page3))
	}
	if page3[0].ID != mkID(109) || page3[1].ID != mkID(110) {
		t.Fatalf("page 3 IDs mismatch: first=%x, last=%x", page3[0].ID, page3[1].ID)
	}
}
