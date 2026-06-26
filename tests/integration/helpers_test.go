package integration

import (
	"encoding/binary"
	"testing"

	"aequitas-ledger/internal/core"
)

func mkID(v uint64) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], v)
	return id
}

// balanceOf asserts the account's invariant holds and returns its balance.
func balanceOf(t testing.TB, a core.Account) core.Uint128 {
	bal, err := core.Balance(a)
	if err != nil {
		t.Fatalf("invariant violation: %v", err)
	}
	return bal
}
