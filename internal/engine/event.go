package engine

import "aequitas-ledger/internal/core"

type TransferEvent struct {
	Transfer core.Transfer
	Result   chan error
}

func NewTransferEvent(t core.Transfer) TransferEvent {
	return TransferEvent{Transfer: t, Result: make(chan error, 1)}
}

type AccountCreateEvent struct {
	Account core.Account
	Result  chan error
}

type ReadAccountEvent struct {
	ID     [16]byte
	Result chan ReadAccountResult
}

type ReadAccountResult struct {
	Account core.Account
	Err     error
}
