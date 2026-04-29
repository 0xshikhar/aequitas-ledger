package core

import "unsafe"

// Transfer is the journal unit. Flag semantics (D1.2, D1.3):
//   - Flags == 0: posts immediately.
//   - FlagPending: opens a hold of Amount against the debit account's
//     available balance; Timeout nanoseconds after Timestamp the hold is
//     automatically voided by the logical clock (0 = never expires).
//   - FlagPostPending: settles the pending transfer with the same ID
//     (Amount = 0 settles the full remaining hold, or a partial amount).
//   - FlagVoidPending: releases the pending transfer with the same ID.
//   - FlagLinked: links this transfer to the next transfer in the batch (D1.3);
//     the entire chain succeeds or fails atomically.
type Transfer struct {
	ID              [16]byte
	DebitAccountID  [16]byte
	CreditAccountID [16]byte
	Amount          Uint128
	IdempotencyKey  [32]byte
	UserData128     [16]byte
	Timestamp       int64
	Timeout         uint64
	Ledger          uint32
	Code            uint16
	_               [2]byte
	Flags           uint32
	_               [4]byte
}

const TransferStructSize = 144

// Compile-time guard: Transfer's size is part of the wire payload contract
// (TransferPayloadSize) and must stay predictable.
var _ [TransferStructSize - unsafe.Sizeof(Transfer{})]byte
