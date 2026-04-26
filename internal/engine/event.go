package engine

import (
	"sync"
	"sync/atomic"

	"aequitas-ledger/internal/core"
)

// One-shot result channels are pooled because every unary request needs
// exactly one buffered (cap 1) channel; at target throughput, per-request
// channel allocation is a dominant hot-path cost (C0.15).
//
// Release rule: a channel may be returned to its pool only after its single
// result has been received, or when the event was never submitted (the loop
// then never sends on it). Releasing an abandoned channel would leak its
// stale result to the next user.
var (
	transferAckPool = sync.Pool{New: func() any { return make(chan TransferAck, 1) }}
	readResultPool  = sync.Pool{New: func() any { return make(chan ReadAccountResult, 1) }}
)

func AcquireTransferAck() chan TransferAck   { return transferAckPool.Get().(chan TransferAck) }
func ReleaseTransferAck(ch chan TransferAck) { transferAckPool.Put(ch) }
func AcquireReadResult() chan ReadAccountResult {
	return readResultPool.Get().(chan ReadAccountResult)
}
func ReleaseReadResult(ch chan ReadAccountResult) { readResultPool.Put(ch) }

// TransferAck is one event's outcome: the committed transfer (with its
// server-assigned timestamp, so callers observe what actually landed) and the
// per-item error, nil on success.
type TransferAck struct {
	Transfer core.Transfer
	Err      error
}

// batchState is the shared completion record of one client transfer batch
// (D1.1): one result slice, one completion notification for the whole batch,
// zero per-transfer channels. Events of one client batch may be applied
// across several engine batches (drained alongside other clients' events),
// so remaining counts down until every event of the batch has been completed.
//
// Concurrency: each result index is written by exactly one goroutine — the
// event loop for submitted events, the submitter only for events it knows
// were never queued (shutdown path). complete is therefore safe from both;
// the atomic counter and once-guarded close make the completion edge exact.
// The submitter reads results only after done closes, whose happens-before
// edge publishes all preceding writes.
type batchState struct {
	results   []TransferAck
	remaining atomic.Int64
	doneOnce  sync.Once
	done      chan struct{}
}

func newBatchState(n int) *batchState {
	bs := &batchState{results: make([]TransferAck, n), done: make(chan struct{})}
	bs.remaining.Store(int64(n))
	return bs
}

func (b *batchState) complete(index int, ack TransferAck) {
	b.results[index] = ack
	if b.remaining.Add(-1) == 0 {
		b.doneOnce.Do(func() { close(b.done) })
	}
}

type TransferEvent struct {
	Transfer core.Transfer
	// Result carries the ack for unary submissions (pooled channel). It is
	// nil for batched submissions, which ack through batch below.
	Result chan TransferAck
	// batch/index link a batched event to its client batch's shared
	// completion state; both are nil/0 for unary events.
	batch *batchState
	index int
}

// complete delivers one outcome: into the batch's shared result slice, or to
// the unary event's own channel. Called from the event-loop goroutine for
// queued events, and from the submitter only for events it never queued.
func (ev *TransferEvent) complete(ack TransferAck) {
	if ev.batch != nil {
		ev.batch.complete(ev.index, ack)
		return
	}
	ev.Result <- ack
}

func NewTransferEvent(t core.Transfer) TransferEvent {
	return TransferEvent{Transfer: t, Result: AcquireTransferAck()}
}

// AccountCreateEvent carries a client batch of accounts (possibly of size 1)
// through the account-create queue: one WAL append + one sync for the whole
// batch, positional results, one completion notification via Done.
type AccountCreateEvent struct {
	Accounts []core.Account
	Results  []error // written only by the event-loop goroutine
	Done     chan struct{}
}

type ReadAccountEvent struct {
	ID     [16]byte
	Result chan ReadAccountResult
}

type ReadAccountResult struct {
	Account core.Account
	Err     error
}

var (
	readTransferResultPool   = sync.Pool{New: func() any { return make(chan ReadTransferResult, 1) }}
	readAccountTransfersPool = sync.Pool{New: func() any { return make(chan []core.Transfer, 1) }}
)

func AcquireReadTransferResult() chan ReadTransferResult {
	return readTransferResultPool.Get().(chan ReadTransferResult)
}
func ReleaseReadTransferResult(ch chan ReadTransferResult) {
	readTransferResultPool.Put(ch)
}
func AcquireReadAccountTransfersResult() chan []core.Transfer {
	return readAccountTransfersPool.Get().(chan []core.Transfer)
}
func ReleaseReadAccountTransfersResult(ch chan []core.Transfer) {
	readAccountTransfersPool.Put(ch)
}

type ReadTransferEvent struct {
	ID     [16]byte
	Result chan ReadTransferResult
}

type ReadTransferResult struct {
	Transfer core.Transfer
	Found    bool
}

type ReadAccountTransfersEvent struct {
	AccountID [16]byte
	AfterID   [16]byte
	Limit     int
	Result    chan []core.Transfer
}

