package core

// Transfer is the journal unit. Flag semantics (D1.2):
//   - Flags == 0: posts immediately.
//   - FlagPending: opens a hold of Amount against the debit account's
//     available balance; Timeout nanoseconds after Timestamp the hold is
//     automatically voided by the logical clock (0 = never expires).
//   - FlagPostPending: settles the pending transfer with the same ID
//     (Amount = 0 settles the full remaining hold, or a partial amount).
//   - FlagVoidPending: releases the pending transfer with the same ID.
type Transfer struct {
	ID              [16]byte
	DebitAccountID  [16]byte
	CreditAccountID [16]byte
	Amount          Uint128
	IdempotencyKey  [32]byte
	Timestamp       int64
	Timeout         uint64
	Flags           uint32
	_               [4]byte
}
