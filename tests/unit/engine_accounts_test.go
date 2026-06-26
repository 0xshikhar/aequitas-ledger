package unit

import (
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
)

func TestAccountManagerBalanceInvariant(t *testing.T) {
	am := engine.NewAccountManager(1)
	id := id16(1)
	acc := core.Account{ID: id, Currency: [4]byte{'U', 'S', 'D', 'C'}}
	if err := am.Create(acc); err != nil {
		t.Fatalf("create account: %v", err)
	}

	for i := 0; i < 1000; i++ {
		amt := core.FromUint64(uint64((i % 7) + 1))
		if err := am.ApplyCredit(id, amt); err != nil {
			t.Fatalf("apply credit: %v", err)
		}
		if i%3 == 0 {
			am.ApplyDebit(id, core.FromUint64(1))
		}
		a, err := am.Get(id)
		if err != nil {
			t.Fatalf("get account: %v", err)
		}
		expected, err := core.Sub(a.PostedCredits, a.PostedDebits)
		if err != nil {
			t.Fatalf("unexpected underflow in expected balance: %v", err)
		}
		if core.Cmp(balanceOf(t, *a), expected) != 0 {
			t.Fatalf("balance invariant broken at i=%d", i)
		}
	}
}

func TestAccountManagerValidateTransfer(t *testing.T) {
	am := engine.NewAccountManager(2)
	debitID := id16(10)
	creditID := id16(11)

	if err := am.Create(core.Account{ID: debitID, Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(50)}); err != nil {
		t.Fatal(err)
	}
	if err := am.Create(core.Account{ID: creditID, Currency: [4]byte{'U', 'S', 'D', 'C'}}); err != nil {
		t.Fatal(err)
	}

	err := am.ValidateTransfer(core.Transfer{
		ID:              id16(99),
		DebitAccountID:  debitID,
		CreditAccountID: creditID,
		Amount:          core.FromUint64(25),
	})
	if err != nil {
		t.Fatalf("expected valid transfer, got %v", err)
	}
}
