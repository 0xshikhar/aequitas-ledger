package engine

import (
	"context"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/observability"
	"aequitas-ledger/internal/wal"
)

type SnapshotRequest struct {
	Result chan SnapshotView
}

type SnapshotView struct {
	Accounts []core.Account
	LSN      int64
}

type EventLoop struct {
	batcher            *Batcher
	accounts           *AccountManager
	wal                *wal.WAL
	currentLSN         int64
	snapshotRequests   chan SnapshotRequest
	readQueue          chan ReadAccountEvent
	accountCreateQueue chan AccountCreateEvent
}

func NewEventLoop(b *Batcher, a *AccountManager, w *wal.WAL) *EventLoop {
	return &EventLoop{
		batcher:            b,
		accounts:           a,
		wal:                w,
		currentLSN:         w.CurrentLSN(),
		snapshotRequests:   make(chan SnapshotRequest, 10),
		readQueue:          make(chan ReadAccountEvent, 1024),
		accountCreateQueue: make(chan AccountCreateEvent, 256),
	}
}

func (l *EventLoop) RequestSnapshotView() ([]core.Account, int64) {
	req := SnapshotRequest{Result: make(chan SnapshotView, 1)}
	l.snapshotRequests <- req
	view := <-req.Result
	return view.Accounts, view.LSN
}

func (l *EventLoop) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			l.abandonPending()
			return
		default:
		}

		l.drainControl()
		batch := l.batcher.Collect(l.controlPending)
		l.drainControl()
		observability.RingBufferDepth.Set(float64(l.batcher.Depth()))

		if len(batch) == 0 {
			continue
		}
		l.processBatch(batch)
	}
}

// abandonPending completes every still-queued transfer event with
// ErrLedgerClosed on shutdown, so submitters waiting on a batch completion
// observe an error instead of hanging forever. Queued account-create batches
// are completed the same way.
func (l *EventLoop) abandonPending() {
	for {
		batch := l.batcher.Collect(nil)
		if len(batch) == 0 {
			break
		}
		for i := range batch {
			observability.TransfersTotal.WithLabelValues("ledger_closed").Inc()
			batch[i].complete(TransferAck{Err: core.ErrLedgerClosed{}})
		}
	}
	for {
		select {
		case req := <-l.accountCreateQueue:
			for i := range req.Results {
				req.Results[i] = core.ErrLedgerClosed{}
			}
			close(req.Done)
		default:
			return
		}
	}
}

// controlPending reports whether read, account-create, or snapshot requests
// are queued. The batcher consults it while waiting so the loop can service
// control work instead of spinning for a fuller batch (C0.16).
func (l *EventLoop) controlPending() bool {
	return len(l.readQueue) > 0 || len(l.accountCreateQueue) > 0 || len(l.snapshotRequests) > 0
}

// drainControl services every currently queued control request so read and
// create latency stays bounded no matter how heavy the transfer load is.
func (l *EventLoop) drainControl() {
	for {
		select {
		case req := <-l.snapshotRequests:
			accs := l.accounts.Snapshot()
			req.Result <- SnapshotView{Accounts: accs, LSN: l.currentLSN}
		case req := <-l.readQueue:
			acc, err := l.accounts.Get(req.ID)
			var a core.Account
			if acc != nil {
				a = *acc
			}
			req.Result <- ReadAccountResult{Account: a, Err: err}
		case req := <-l.accountCreateQueue:
			l.processAccountCreate(req)
		default:
			return
		}
	}
}

// processAccountCreate applies one client batch of account creations: one
// WAL append + one sync for the whole batch, positional results, a single
// completion notification via req.Done.
func (l *EventLoop) processAccountCreate(req AccountCreateEvent) {
	// Duplicates never enter the WAL batch; they fail per-item.
	var recs []wal.Record
	var walIdx []int
	for i := range req.Accounts {
		if _, err := l.accounts.Get(req.Accounts[i].ID); err == nil {
			req.Results[i] = core.ErrDuplicateAccountID{AccountID: req.Accounts[i].ID}
			continue
		}
		recs = append(recs, wal.Record{Type: wal.RecordTypeAccount, Payload: EncodeAccountPayload(req.Accounts[i])})
		walIdx = append(walIdx, i)
	}
	if len(recs) == 0 {
		close(req.Done)
		return
	}

	if _, err := l.wal.AppendBatch(recs); err != nil {
		for _, i := range walIdx {
			req.Results[i] = err
		}
		close(req.Done)
		return
	}
	if err := l.wal.Sync(); err != nil {
		for _, i := range walIdx {
			req.Results[i] = err
		}
		close(req.Done)
		return
	}
	l.currentLSN = l.wal.CurrentLSN()
	for _, i := range walIdx {
		req.Results[i] = l.accounts.Create(req.Accounts[i])
	}
	close(req.Done)
}

func (l *EventLoop) processBatch(events []TransferEvent) {
	start := time.Now()
	nEvents := len(events)
	observability.BatchSize.Observe(float64(nEvents))

	outcomes := ValidateBatch(events, l.accounts)
	records := makeWALRecords(events, outcomes)

	if len(records) > 0 {
		if _, err := l.wal.AppendBatch(records); err != nil {
			for i := range events {
				observability.TransfersTotal.WithLabelValues("wal_append_error").Inc()
				events[i].complete(TransferAck{Err: err})
			}
			return
		}
		syncStart := time.Now()
		if err := l.wal.Sync(); err != nil {
			observability.WALSyncDuration.Observe(time.Since(syncStart).Seconds())
			for i := range events {
				observability.TransfersTotal.WithLabelValues("wal_sync_error").Inc()
				events[i].complete(TransferAck{Err: err})
			}
			return
		}
		observability.WALSyncDuration.Observe(time.Since(syncStart).Seconds())
		l.currentLSN = l.wal.CurrentLSN()
	}

	ApplyBatch(events, outcomes, l.accounts)
	for i := range events {
		if outcomes[i] != nil {
			observability.TransfersTotal.WithLabelValues("failed").Inc()
		} else {
			observability.TransfersTotal.WithLabelValues("success").Inc()
		}
		// The ack carries the loop's copy of the transfer — for successful
		// items it includes the server-assigned timestamp written by
		// makeWALRecords.
		events[i].complete(TransferAck{Transfer: events[i].Transfer, Err: outcomes[i]})
	}
	observability.TransferDuration.Observe(time.Since(start).Seconds())
}

func makeWALRecords(events []TransferEvent, outcomes []error) []wal.Record {
	recs := make([]wal.Record, 0, len(events))
	now := time.Now().UnixNano()
	for i := range events {
		if outcomes[i] != nil {
			continue
		}
		t := events[i].Transfer
		if t.Timestamp == 0 {
			t.Timestamp = now
			now++
		}
		recs = append(recs, wal.Record{Type: wal.RecordTypeTransfer, Payload: EncodeTransferPayload(t)})
		events[i].Transfer = t
	}
	return recs
}
