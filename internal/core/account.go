package core

import "unsafe"

const (
	AccountFlagFrozen uint32 = 1 << iota
	AccountFlagClosed
)

type Account struct {
	ID            [16]byte
	Currency      [4]byte
	PostedDebits  Uint128
	PostedCredits Uint128
	Flags         uint32
	_             [4]byte
}

func Balance(a Account) Uint128 {
	bal, err := Sub(a.PostedCredits, a.PostedDebits)
	if err != nil {
		panic("account invariant violated: posted debits exceed posted credits")
	}
	return bal
}

func IsFrozen(a Account) bool {
	return a.Flags&AccountFlagFrozen != 0
}

func IsClosed(a Account) bool {
	return a.Flags&AccountFlagClosed != 0
}

// Compile-time guard to keep Account cache-friendly and predictable.
var _ [64 - unsafe.Sizeof(Account{})]byte
