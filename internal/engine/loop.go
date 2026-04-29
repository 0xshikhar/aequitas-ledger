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
	Pendings []core.Transfer
	LSN      int64
}

type EventLoop struct {
	batcher            *Batcher
	accounts           *AccountManager
	wal                *wal.WAL
	currentLSN         int64
	clock              int64
	snapshotRequests          chan SnapshotRequest
	readQueue                 chan ReadAccountEvent
	readTransferQueue         chan ReadTransferEvent
	readAccountTransfersQueue chan ReadAccountTransfersEvent
	accountCreateQueue        chan AccountCreateEvent
	// closed is closed exactly once when Run exits; callers blocked on loop
	// round-trips (snapshot views) use it to give up instead of hanging.
	closed chan struct{}

	// Reusable per-batch scratch (loop-goroutine-owned; the WAL copies the
	// encoded bytes before AppendBatch returns, so reuse is safe).
	recordsBuf []wal.Record
	payloadBuf []byte
}

func NewEventLoop(b *Batcher, a *AccountManager, w *wal.WAL) *EventLoop {
	return &EventLoop{
		batcher:            b,
		accounts:           a,
		wal:                w,
		currentLSN:         w.CurrentLSN(),
		snapshotRequests:          make(chan SnapshotRequest, 10),
		readQueue:                 make(chan ReadAccountEvent, 1024),
		readTransferQueue:         make(chan ReadTransferEvent, 1024),
		readAccountTransfersQueue: make(chan ReadAccountTransfersEvent, 1024),
		accountCreateQueue:        make(chan AccountCreateEvent, 256),
		closed:                    make(chan struct{}),
	}
}

func (l *EventLoop) RequestSnapshotView() ([]core.Account, []core.Transfer, int64) {
	req := SnapshotRequest{Result: make(chan SnapshotView, 1)}
	select {
	case l.snapshotRequests <- req:
		select {
		case view := <-req.Result:
			return view.Accounts, view.Pendings, view.LSN
		case <-l.closed:
			// Loop exited before servicing the request.
			return nil, nil, 0
		}
	case <-l.closed:
		return nil, nil, 0
	}
}

func (l *EventLoop) Run(ctx context.Context) {
	defer close(l.closed)
	for {
		select {
		case <-ctx.Done():
			l.abandonPending()
			return
		default:
		}

		l.drainControl()

		// Idle: block on every real wakeup source — producer arrivals,
		// reads, account creates, snapshot views, shutdown. This replaced
		// the busy-spin with a zero-CPU, zero-latency wait (S2.1).
		if l.batcher.Idle() {
			if !l.waitForWork(ctx) {
				l.abandonPending()
				return
			}
			l.drainControl()
			if l.batcher.Idle() {
				continue
			}
		}

		batch := l.batcher.Collect(l.controlPending)
		l.drainControl()
		observability.RingBufferDepth.Set(float64(l.batcher.Depth()))

		if len(batch) == 0 {
			continue
		}
		l.processBatch(batch)
	}
}

// waitForWork blocks until any wakeup source fires. It returns false only on
// shutdown. Control requests that arrive while idle are serviced inline.
func (l *EventLoop) waitForWork(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-l.batcher.ArriveChan():
		return true
	case req := <-l.readQueue:
		l.handleRead(req)
		return true
	case req := <-l.readTransferQueue:
		l.handleReadTransfer(req)
		return true
	case req := <-l.readAccountTransfersQueue:
		l.handleReadAccountTransfers(req)
		return true
	case req := <-l.accountCreateQueue:
		l.processAccountCreate(req)
		return true
	case req := <-l.snapshotRequests:
		accs := l.accounts.Snapshot()
		req.Result <- SnapshotView{Accounts: accs, Pendings: l.accounts.PendingTransfers(), LSN: l.currentLSN}
		return true
	}
}

// handleRead answers one queued read (shared by drainControl and the idle
// wait).
func (l *EventLoop) handleRead(req ReadAccountEvent) {
	acc, err := l.accounts.Get(req.ID)
	var a core.Account
	if acc != nil {
		a = *acc
	}
	req.Result <- ReadAccountResult{Account: a, Err: err}
}

func (l *EventLoop) handleReadTransfer(req ReadTransferEvent) {
	t, ok := l.accounts.GetTransfer(req.ID)
	req.Result <- ReadTransferResult{Transfer: t, Found: ok}
}

func (l *EventLoop) handleReadAccountTransfers(req ReadAccountTransfersEvent) {
	ts := l.accounts.AccountTransfers(req.AccountID, req.AfterID, req.Limit)
	req.Result <- ts
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
		case req := <-l.snapshotRequests:
			// Shutdown drain: reply empty so the requester sees LSN 0
			// ("nothing to snapshot") instead of hanging.
			req.Result <- SnapshotView{}
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
	return len(l.readQueue) > 0 || len(l.readTransferQueue) > 0 || len(l.readAccountTransfersQueue) > 0 || len(l.accountCreateQueue) > 0 || len(l.snapshotRequests) > 0
}

// drainControl services every currently queued control request so read and
// create latency stays bounded no matter how heavy the transfer load is.
func (l *EventLoop) drainControl() {
	for {
		select {
		case req := <-l.snapshotRequests:
			accs := l.accounts.Snapshot()
			req.Result <- SnapshotView{Accounts: accs, Pendings: l.accounts.PendingTransfers(), LSN: l.currentLSN}
		case req := <-l.readQueue:
			l.handleRead(req)
		case req := <-l.readTransferQueue:
			l.handleReadTransfer(req)
		case req := <-l.readAccountTransfersQueue:
			l.handleReadAccountTransfers(req)
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
	// Invariant-broken and duplicate accounts never enter the WAL batch;
	// they fail per-item.
	l.recordsBuf = l.recordsBuf[:0]
	l.payloadBuf = l.payloadBuf[:0]
	var walIdx []int
	for i := range req.Accounts {
		if err := ValidateAccount(req.Accounts[i]); err != nil {
			req.Results[i] = err
			continue
		}
		if _, err := l.accounts.Get(req.Accounts[i].ID); err == nil {
			req.Results[i] = core.ErrDuplicateAccountID{AccountID: req.Accounts[i].ID}
			continue
		}
		mark := len(l.payloadBuf)
		l.payloadBuf = core.AppendAccountPayload(l.payloadBuf, req.Accounts[i])
		l.recordsBuf = append(l.recordsBuf, wal.Record{Type: wal.RecordTypeAccount, Payload: l.payloadBuf[mark:]})
		walIdx = append(walIdx, i)
	}
	recs := l.recordsBuf
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

	// Batch clock (D1.2, D1.6): one timestamp per batch, driving hold expiry on
	// the logical clock so replay decisions match live ones exactly.
	// Timestamp batches once at the batch boundary rather than calling time.Now()
	// per transfer, guaranteeing identical timestamps across replay and follower nodes.
	now := time.Now().UnixNano()
	if now <= l.clock {
		now = l.clock + 1
	}
	l.clock = now
	l.accounts.SetClock(l.clock)
	l.accounts.CloseExpired()

	// Batch-synchronous timestamp discipline (D1.6): stamp every transfer in the
	// batch upfront before validation and journaling.
	for i := range events {
		events[i].Transfer.Timestamp = l.clock
	}

	outcomes := ValidateBatch(events, l.accounts)
	records := l.makeWALRecords(events, outcomes)

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

// makeWALRecords builds the WAL records for the batch into loop-owned
// scratch buffers: zero allocations per transfer (S2.1). The WAL copies the
// encoded payload bytes before AppendBatch returns, so the buffers can be
// reused for the next batch.
func (l *EventLoop) makeWALRecords(events []TransferEvent, outcomes []error) []wal.Record {
	l.recordsBuf = l.recordsBuf[:0]
	l.payloadBuf = l.payloadBuf[:0]
	for i := range events {
		if outcomes[i] != nil {
			continue
		}
		t := events[i].Transfer
		mark := len(l.payloadBuf)
		l.payloadBuf = core.AppendTransferPayload(l.payloadBuf, t)
		l.recordsBuf = append(l.recordsBuf, wal.Record{Type: wal.RecordTypeTransfer, Payload: l.payloadBuf[mark:]})
	}
	return l.recordsBuf
}
