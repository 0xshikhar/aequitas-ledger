package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/snapshot"
	"aequitas-ledger/internal/wal"
)

type Config struct {
	RingBufferSize         int
	MaxBatchSize           int
	BatchTimeout           time.Duration
	InitialAccountCapacity int
	IdempotencyMaxSize     int
	IdempotencyTTL         time.Duration
	InitialAccounts        []core.Account
	SnapshotDir            string
	IsFollower             bool
}

func DefaultConfig() Config {
	return Config{
		RingBufferSize:         1 << 16,
		MaxBatchSize:           10_000,
		BatchTimeout:           time.Millisecond,
		InitialAccountCapacity: 1024,
		IdempotencyMaxSize:     100_000,
		IdempotencyTTL:         15 * time.Minute,
	}
}

type Ledger struct {
	loop        *EventLoop
	rb          *RingBuffer
	idempKey    *IdempotencyStore
	accounts    *AccountManager
	wal         *wal.WAL
	snapshotDir string
	isFollower  bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewLedger(cfg Config, w *wal.WAL) (*Ledger, error) {
	if w == nil {
		return nil, errors.New("nil wal")
	}
	if cfg.RingBufferSize == 0 {
		cfg = DefaultConfig()
	}

	snapshotDir := cfg.SnapshotDir
	if snapshotDir == "" {
		snapshotDir = filepath.Join(w.Dir(), "snapshots")
	}

	accounts := NewAccountManager(cfg.InitialAccountCapacity)
	if w.CurrentLSN() == 0 && len(cfg.InitialAccounts) > 0 {
		var initRecs []wal.Record
		for _, a := range cfg.InitialAccounts {
			payload := EncodeAccountPayload(a)
			initRecs = append(initRecs, wal.Record{Type: wal.RecordTypeAccount, Payload: payload})
		}
		if _, err := w.AppendBatch(initRecs); err != nil {
			return nil, fmt.Errorf("failed to write initial accounts to WAL: %w", err)
		}
		if err := w.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync initial accounts WAL: %w", err)
		}
	}

	var snapshotLSN int64 = 0
	if latestPath, _, err := snapshot.Latest(snapshotDir); err == nil && latestPath != "" {
		snapAccounts, snapLSNRead, rerr := snapshot.Read(latestPath)
		if rerr == nil {
			for _, a := range snapAccounts {
				_ = accounts.Create(a)
			}
			snapshotLSN = snapLSNRead
		}
	}

	l := &Ledger{
		rb:          NewRingBuffer(cfg.RingBufferSize),
		idempKey:    NewIdempotencyStore(cfg.IdempotencyMaxSize, cfg.IdempotencyTTL),
		accounts:    accounts,
		wal:         w,
		snapshotDir: snapshotDir,
		isFollower:  cfg.IsFollower,
	}
	if err := l.RecoverFromSnapshot(snapshotLSN); err != nil {
		return nil, err
	}

	batcher := NewBatcher(l.rb, cfg.MaxBatchSize, cfg.BatchTimeout)
	l.loop = NewEventLoop(batcher, l.accounts, l.wal)
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.loop.Run(l.ctx)
	}()
	return l, nil
}

func (l *Ledger) IsFollower() bool {
	return l.isFollower
}

func (l *Ledger) Close() error {
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()
	return l.wal.Close()
}

func (l *Ledger) CreateTransfer(ctx context.Context, t core.Transfer) (core.Transfer, error) {
	if l.isFollower {
		return core.Transfer{}, core.ErrNotLeader{}
	}
	key := t.IdempotencyKey
	for {
		existing, found, err := l.idempKey.CheckAndReserve(key)
		if err != nil {
			var conflict core.ErrIdempotencyConflict
			if errors.As(err, &conflict) {
				select {
				case <-ctx.Done():
					return core.Transfer{}, ctx.Err()
				default:
					runtime.Gosched()
					continue
				}
			}
			return core.Transfer{}, err
		}
		if found {
			return existing, nil
		}
		break
	}

	ev := NewTransferEvent(t)
	if err := l.rb.Submit(ev); err != nil {
		l.idempKey.Rollback(key)
		return core.Transfer{}, err
	}

	select {
	case err := <-ev.Result:
		if err != nil {
			l.idempKey.Rollback(key)
			return core.Transfer{}, err
		}
		l.idempKey.Commit(key, ev.Transfer)
		return ev.Transfer, nil
	case <-ctx.Done():
		l.idempKey.Rollback(key)
		return core.Transfer{}, ctx.Err()
	}
}

func (l *Ledger) CreateAccount(ctx context.Context, acc core.Account) (core.Account, error) {
	if l.isFollower {
		return core.Account{}, core.ErrNotLeader{}
	}
	resChan := make(chan error, 1)
	ev := AccountCreateEvent{Account: acc, Result: resChan}

	select {
	case l.loop.accountCreateQueue <- ev:
	case <-ctx.Done():
		return core.Account{}, ctx.Err()
	}

	select {
	case err := <-resChan:
		if err != nil {
			return core.Account{}, err
		}
		return acc, nil
	case <-ctx.Done():
		return core.Account{}, ctx.Err()
	}
}

func (l *Ledger) GetAccount(ctx context.Context, id [16]byte) (core.Account, error) {
	resChan := make(chan ReadAccountResult, 1)
	ev := ReadAccountEvent{ID: id, Result: resChan}

	select {
	case l.loop.readQueue <- ev:
	case <-ctx.Done():
		return core.Account{}, ctx.Err()
	}

	select {
	case res := <-resChan:
		return res.Account, res.Err
	case <-ctx.Done():
		return core.Account{}, ctx.Err()
	}
}

func (l *Ledger) GetBalance(ctx context.Context, id [16]byte) (core.Uint128, error) {
	acc, err := l.GetAccount(ctx, id)
	if err != nil {
		return core.Uint128{}, err
	}
	return core.Balance(acc), nil
}

func (l *Ledger) TriggerSnapshot(ctx context.Context) (int64, error) {
	if l.loop == nil {
		return 0, nil
	}
	accs, lsn := l.loop.RequestSnapshotView()
	if lsn <= 0 {
		return 0, nil
	}
	snapPath := filepath.Join(l.snapshotDir, fmt.Sprintf("snapshot-%d.snap", lsn))
	if err := snapshot.Write(snapPath, lsn, accs); err != nil {
		return 0, fmt.Errorf("write snapshot: %w", err)
	}
	_ = snapshot.CleanupOldSnapshots(l.snapshotDir, 3)
	if err := l.wal.TruncateBefore(lsn); err != nil {
		return lsn, fmt.Errorf("truncate wal after snapshot: %w", err)
	}
	return lsn, nil
}

func (l *Ledger) Recover() error {
	return l.RecoverFromSnapshot(0)
}

func (l *Ledger) RecoverFromSnapshot(fromLSN int64) error {
	return l.wal.RecoverFromLSN(fromLSN, func(r wal.Record) error {
		switch r.Type {
		case wal.RecordTypeAccount:
			acc, err := DecodeAccountPayload(r.Payload)
			if err != nil {
				return err
			}
			err = l.accounts.Create(acc)
			var dup core.ErrDuplicateAccountID
			if errors.As(err, &dup) {
				return nil
			}
			return err
		case wal.RecordTypeTransfer:
			t, err := DecodeTransferPayload(r.Payload)
			if err != nil {
				return err
			}
			events := []TransferEvent{{Transfer: t}}
			outcomes := []error{nil}
			ApplyBatch(events, outcomes, l.accounts)
			if outcomes[0] != nil {
				return outcomes[0]
			}
			if t.IdempotencyKey != [32]byte{} {
				l.idempKey.Commit(t.IdempotencyKey, t)
			}
			return nil
		default:
			return nil
		}
	})
}
