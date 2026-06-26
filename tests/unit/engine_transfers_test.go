package unit

import (
	"errors"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
)

func TestTransfersBatchInterleavedValidityAndOrdering(t *testing.T) {
	am := engine.NewAccountManager(3)
	a := id16(1)
	b := id16(2)
	c := id16(3)

	mustCreate(t, am, core.Account{ID: a, Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(100)})
	mustCreate(t, am, core.Account{ID: b, Currency: [4]byte{'U', 'S', 'D', 'C'}})
	mustCreate(t, am, core.Account{ID: c, Currency: [4]byte{'U', 'S', 'D', 'C'}})

	events := []engine.TransferEvent{
		engine.NewTransferEvent(core.Transfer{ID: id16(101), DebitAccountID: a, CreditAccountID: b, Amount: core.FromUint64(80)}),
		engine.NewTransferEvent(core.Transfer{ID: id16(102), DebitAccountID: a, CreditAccountID: a, Amount: core.FromUint64(1)}),
		engine.NewTransferEvent(core.Transfer{ID: id16(103), DebitAccountID: a, CreditAccountID: c, Amount: core.FromUint64(30)}),
	}

	outcomes := engine.ValidateBatch(events, am)
	engine.ApplyBatch(events, outcomes, am)

	if outcomes[0] != nil {
		t.Fatalf("event 0 should be valid, got %v", outcomes[0])
	}
	var selfErr core.ErrSelfTransfer
	if !errors.As(outcomes[1], &selfErr) {
		t.Fatalf("event 1 should be ErrSelfTransfer, got %v", outcomes[1])
	}
	var fundsErr core.ErrInsufficientFunds
	if !errors.As(outcomes[2], &fundsErr) {
		t.Fatalf("event 2 should fail post-ordering funds check, got %v", outcomes[2])
	}

	aa, _ := am.Get(a)
	bb, _ := am.Get(b)
	cc, _ := am.Get(c)

	if got := core.String(balanceOf(t, *aa)); got != "20" {
		t.Fatalf("unexpected A balance: %s", got)
	}
	if got := core.String(balanceOf(t, *bb)); got != "80" {
		t.Fatalf("unexpected B balance: %s", got)
	}
	if got := core.String(balanceOf(t, *cc)); got != "0" {
		t.Fatalf("unexpected C balance: %s", got)
	}
}
