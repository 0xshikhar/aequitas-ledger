package core

import "unsafe"

const (
	AccountFlagFrozen uint32 = 1 << iota
	AccountFlagClosed
)

// Transfer flag semantics (D1.2, TigerBeetle-style two-phase transfers):
// a plain transfer posts immediately; FlagPending opens a hold; a transfer
// carrying FlagPostPending or FlagVoidPending with the SAME id as an open
// pending transfer settles or releases it (optionally partially, by Amount).
const (
	TransferFlagPending       uint32 = 1 << iota
	TransferFlagPostPending
	TransferFlagVoidPending
)

// Account carries posted balances plus pending (hold) balances. A pending
// debit reserves funds before they move; PendingCredits is the mirror side
// for holds that credit an account (e.g. an expected inbound settlement).
type Account struct {
	ID             [16]byte
	Currency       [4]byte
	PostedDebits   Uint128
	PostedCredits  Uint128
	PendingDebits  Uint128
	PendingCredits Uint128
	Flags          uint32
	_              [4]byte
}

// AccountPayloadSize must match Account's in-memory size: the compile-time
// guard below enforces it.
const AccountStructSize = 96

// Balance returns posted credits − posted debits. A negative balance means
// the account's stored state violates the ledger's non-negative-balance
// invariant, which is reported as ErrInvariantViolation rather than panicked:
// callers decide the policy (fail the transfer item, refuse the read, reject
// the account), and the event loop increments the invariant-violations metric.
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

// AvailableBalance is what a new hold may still reserve: posted balance
// minus outstanding pending debits plus outstanding pending credits.
func AvailableBalance(a Account) (Uint128, error) {
	posted, err := Balance(a)
	if err != nil {
		return Uint128{}, err
	}
	avail, err := Sub(posted, a.PendingDebits)
	if err != nil {
		return Uint128{}, ErrInvariantViolation{
			AccountID:    a.ID,
			PostedDebits: a.PostedDebits,
		}
	}
	if avail, err = Add(avail, a.PendingCredits); err != nil {
		return Uint128{}, ErrBalanceOverflow{AccountID: a.ID}
	}
	return avail, nil
}

func IsFrozen(a Account) bool {
	return a.Flags&AccountFlagFrozen != 0
}

func IsClosed(a Account) bool {
	return a.Flags&AccountFlagClosed != 0
}

// Compile-time guard: Account's size is part of the wire payload contract
// (AccountPayloadSize) and must stay predictable.
var _ [AccountStructSize - unsafe.Sizeof(Account{})]byte
