package engine

import (
	"context"
	"time"

	"aequitas-ledger/internal/wal"
)

type EventLoop struct {
	batcher    *Batcher
	accounts   *AccountManager
	wal        *wal.WAL
	currentLSN int64
}

func NewEventLoop(b *Batcher, a *AccountManager, w *wal.WAL) *EventLoop {
	return &EventLoop{batcher: b, accounts: a, wal: w}
}

func (l *EventLoop) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch := l.batcher.Collect()
		if len(batch) == 0 {
			continue
		}
		l.processBatch(batch)
	}
}

func (l *EventLoop) processBatch(events []TransferEvent) {
	outcomes := ValidateBatch(events, l.accounts)
	records := makeWALRecords(events, outcomes)

	if len(records) > 0 {
		if _, err := l.wal.AppendBatch(records); err != nil {
			for i := range events {
				events[i].Result <- err
			}
			return
		}
		if err := l.wal.Sync(); err != nil {
			for i := range events {
				events[i].Result <- err
			}
			return
		}
		l.currentLSN = l.wal.CurrentLSN()
	}

	ApplyBatch(events, outcomes, l.accounts)
	for i := range events {
		events[i].Result <- outcomes[i]
	}
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
		recs = append(recs, wal.Record{Type: wal.RecordTypeTransfer, Payload: encodeTransferPayload(t)})
		events[i].Transfer = t
	}
	return recs
}
