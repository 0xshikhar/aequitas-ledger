package engine

import (
	"sync"

	"aequitas-ledger/internal/core"
)

// One-shot result channels are pooled because every request needs exactly one
// buffered (cap 1) channel; at target throughput, per-request channel
// allocation is a dominant hot-path cost (C0.15).
//
// Release rule: a channel may be returned to its pool only after its single
// result has been received, or when the event was never submitted (the loop
// then never sends on it). Releasing an abandoned channel would leak its
// stale result to the next user.
var (
	errResultPool  = sync.Pool{New: func() any { return make(chan error, 1) }}
	readResultPool = sync.Pool{New: func() any { return make(chan ReadAccountResult, 1) }}
)

func AcquireErrorResult() chan error   { return errResultPool.Get().(chan error) }
func ReleaseErrorResult(ch chan error) { errResultPool.Put(ch) }
func AcquireReadResult() chan ReadAccountResult {
	return readResultPool.Get().(chan ReadAccountResult)
}
func ReleaseReadResult(ch chan ReadAccountResult) { readResultPool.Put(ch) }

type TransferEvent struct {
	Transfer core.Transfer
	Result   chan error
}

func NewTransferEvent(t core.Transfer) TransferEvent {
	return TransferEvent{Transfer: t, Result: AcquireErrorResult()}
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
