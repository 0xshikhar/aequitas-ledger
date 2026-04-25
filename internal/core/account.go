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

// Balance returns credits − debits. A negative balance means the account's
// stored state violates the ledger's non-negative-balance invariant, which is
// reported as ErrInvariantViolation rather than panicked: callers decide the
// policy (fail the transfer item, refuse the read, reject the account), and
// the event loop increments the invariant-violations metric.
func Balance(a Account) (Uint128, error) {
	bal, err := Sub(a.PostedCredits, a.PostedDebits)
	if err != nil {
		return Uint128{}, ErrInvariantViolation{
			AccountID:     a.ID,
			PostedDebits:  a.PostedDebits,
			PostedCredits: a.PostedCredits,
		}
	}
	return bal, nil
}

func IsFrozen(a Account) bool {
	return a.Flags&AccountFlagFrozen != 0
}

func IsClosed(a Account) bool {
	return a.Flags&AccountFlagClosed != 0
}

// Compile-time guard to keep Account cache-friendly and predictable.
var _ [64 - unsafe.Sizeof(Account{})]byte
