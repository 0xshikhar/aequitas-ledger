package unit

import (
	"encoding/binary"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
)

func id16(v uint64) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], v)
	return id
}

func mustCreate(t *testing.T, am *engine.AccountManager, a core.Account) {
	t.Helper()
	if err := am.Create(a); err != nil {
		t.Fatalf("create account failed: %v", err)
	}
}

// balanceOf asserts the account's invariant holds and returns its balance.
func balanceOf(t testing.TB, a core.Account) core.Uint128 {
	bal, err := core.Balance(a)
	if err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
	return bal
}
