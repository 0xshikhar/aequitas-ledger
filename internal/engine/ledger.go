package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/observability"
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
	// SnapshotInterval > 0 starts the background checkpointer (C0.5);
	// 0 disables it. MaxSnapshotsKept bounds retention (default 3).
	SnapshotInterval time.Duration
	MaxSnapshotsKept int
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

	recovered       atomic.Bool // state replay finished; serving is safe
	checkpointOn    atomic.Bool // background checkpointer configured
	snapshotCycleOK atomic.Bool // at least one successful snapshot cycle ran
	lastSnapshotLSN atomic.Int64

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
			if err := ValidateAccount(a); err != nil {
				return nil, fmt.Errorf("initial account %x: %w", a.ID, err)
			}
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
		snapAccounts, snapPendings, snapLSNRead, rerr := snapshot.Read(latestPath)
		if rerr == nil {
			for _, a := range snapAccounts {
				_ = accounts.Create(a)
			}
			for _, p := range snapPendings {
				if err := accounts.RestorePending(p); err != nil {
					return nil, fmt.Errorf("restore pending transfer: %w", err)
				}
				if p.Timestamp > accounts.clock {
					accounts.clock = p.Timestamp
				}
			}
			snapshotLSN = snapLSNRead
		}
	}

	rb, err := NewRingBuffer(cfg.RingBufferSize)
	if err != nil {
		return nil, fmt.Errorf("invalid engine config: %w", err)
	}

	l := &Ledger{
		rb:          rb,
		idempKey:    NewIdempotencyStore(cfg.IdempotencyMaxSize, cfg.IdempotencyTTL),
		accounts:    accounts,
		wal:         w,
		snapshotDir: snapshotDir,
		isFollower:  cfg.IsFollower,
	}
	if err := l.RecoverFromSnapshot(snapshotLSN); err != nil {
		return nil, err
	}
	// The snapshot is authoritative up to its LSN: even when the surviving WAL
	// is shorter (e.g. segments lost after the snapshot was persisted), new
	// records must never reuse LSNs at or below it.
	if snapshotLSN > 0 {
		l.wal.AdvanceLSNTo(snapshotLSN)
	}

	batcher := NewBatcher(l.rb, cfg.MaxBatchSize, cfg.BatchTimeout)
	l.loop = NewEventLoop(batcher, l.accounts, l.wal)
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.loop.Run(l.ctx)
	}()
	l.recovered.Store(true)
	if cfg.SnapshotInterval > 0 {
		l.StartCheckpointer(cfg.SnapshotInterval, cfg.MaxSnapshotsKept)
	}
	return l, nil
}

// StartCheckpointer launches the background snapshot loop (C0.5): on every
// interval it takes a snapshot via TriggerSnapshot — which persists state,
// prunes old snapshots, and truncates WAL segments below the snapshot LSN —
// so production WALs stop growing without bound. The first cycle runs
// immediately (an empty ledger completes the cycle without writing a file).
// The goroutine shares the Ledger's lifecycle: Close cancels and waits for it
// before closing the WAL.
func (l *Ledger) StartCheckpointer(interval time.Duration, maxKept int) {
	if interval <= 0 || l.checkpointOn.Swap(true) {
		return
	}
	if maxKept <= 0 {
		maxKept = 3
	}

	// First cycle runs synchronously so readiness is deterministic by the
	// time StartCheckpointer returns (an empty ledger completes without a
	// file); subsequent cycles run on the ticker goroutine.
	takeSnapshot := func() {
		lsn, err := l.TriggerSnapshot(context.Background())
		if err != nil {
			slog.Warn("background snapshot failed", "error", err)
			return // cycle not complete; readiness stays false
		}
		if lsn > 0 {
			l.lastSnapshotLSN.Store(lsn)
			observability.LastSnapshotLSN.Set(float64(lsn))
		}
		l.snapshotCycleOK.Store(true)
	}
	takeSnapshot()

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-l.ctx.Done():
				return
			case <-ticker.C:
				takeSnapshot()
			}
		}
	}()
}

// Ready reports whether the node can safely serve traffic: state recovery is
// complete and, when a checkpointer is configured, at least one snapshot
// cycle has succeeded (so WAL retention is actually running).
func (l *Ledger) Ready() bool {
	if !l.recovered.Load() {
		return false
	}
	if l.checkpointOn.Load() && !l.snapshotCycleOK.Load() {
		return false
	}
	return true
}

// LastSnapshotLSN reports the snapshot watermark recorded by the background
// checkpointer (0 when none has been taken).
func (l *Ledger) LastSnapshotLSN() int64 {
	return l.lastSnapshotLSN.Load()
}

func (l *Ledger) IsFollower() bool {
	return l.isFollower
}

// CurrentLSN exposes the WAL head watermark: the LSN through which in-memory
// state is known to be applied (snapshot and/or replayed records).
func (l *Ledger) CurrentLSN() int64 {
	return l.wal.CurrentLSN()
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
	// A zero idempotency key means the caller opted out of deduplication —
	// matching the recovery path, which only rebuilds keys for non-zero-key
	// transfers. Deduping the zero key would collapse distinct keyless
	// transfers into the first one's result.
	if t.IdempotencyKey != [32]byte{} {
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
			ReleaseTransferAck(ev.Result) // never queued; the loop cannot send on it
			return core.Transfer{}, err
		}

		select {
		case ack := <-ev.Result:
			ReleaseTransferAck(ev.Result) // drained; safe to recycle
			if ack.Err != nil {
				l.idempKey.Rollback(key)
				return core.Transfer{}, ack.Err
			}
			l.idempKey.Commit(key, ack.Transfer)
			return ack.Transfer, nil
		case <-ctx.Done():
			l.idempKey.Rollback(key)
			// Abandoned mid-flight: the loop may still deliver a result, so the
			// channel must not be recycled.
			return core.Transfer{}, ctx.Err()
		}
	}

	ev := NewTransferEvent(t)
	if err := l.rb.Submit(ev); err != nil {
		ReleaseTransferAck(ev.Result) // never queued; the loop cannot send on it
		return core.Transfer{}, err
	}

	select {
	case ack := <-ev.Result:
		ReleaseTransferAck(ev.Result) // drained; safe to recycle
		return ack.Transfer, ack.Err
	case <-ctx.Done():
		// Abandoned mid-flight: the loop may still deliver a result, so the
		// channel must not be recycled.
		return core.Transfer{}, ctx.Err()
	}
}

func (l *Ledger) CreateAccount(ctx context.Context, acc core.Account) (core.Account, error) {
	if l.isFollower {
		return core.Account{}, core.ErrNotLeader{}
	}
	outcomes, err := l.CreateAccounts(ctx, []core.Account{acc})
	if err != nil {
		return core.Account{}, err
	}
	if outcomes[0].Err != nil {
		return core.Account{}, outcomes[0].Err
	}
	return outcomes[0].Account, nil
}

// TransferOutcome is the per-item result of a transfer batch: Err is nil on
// success, in which case Transfer is the committed transfer (including its
// server-assigned timestamp).
type TransferOutcome struct {
	Transfer core.Transfer
	Err      error
}

// AccountOutcome is the per-item result of an account batch.
type AccountOutcome struct {
	Account core.Account
	Err     error
}

// MaxTransferBatchSize and MaxAccountBatchSize bound one client batch, the
// same role TigerBeetle's ~8k-items-per-message limit plays.
const (
	MaxTransferBatchSize = 8192
	MaxAccountBatchSize  = 8192
)

// CreateTransfers submits a client batch through the single-writer engine
// with one shared completion: N ring-buffer submissions, one result slice,
// one notification (D1.1). Events of the batch may be applied across several
// engine batches alongside other clients' events; the batch's outcome is
// ready when its last event lands.
//
// Semantics:
//   - Idempotency is checked per item before submission; zero keys opt out.
//     Items whose key was already seen return the original transfer.
//   - Once any event is queued, the batch runs to completion: cancelling the
//     request context does NOT undo it (mirrors TigerBeetle, where a sent
//     message is committed and its results read from the completion). The
//     wait is bounded by one engine batch cycle.
//   - On ledger shutdown, events that were never queued are completed with
//     ErrLedgerClosed; queued ones either apply or fail with it.
func (l *Ledger) CreateTransfers(ctx context.Context, batch []core.Transfer) ([]TransferOutcome, error) {
	if l.isFollower {
		return nil, core.ErrNotLeader{}
	}
	if len(batch) == 0 {
		return nil, nil
	}
	if len(batch) > MaxTransferBatchSize {
		return nil, core.ErrBatchTooLarge{Got: len(batch), Max: MaxTransferBatchSize}
	}

	outcomes := make([]TransferOutcome, len(batch))
	events := make([]TransferEvent, 0, len(batch))
	positions := make([]int, 0, len(batch))
	var reservedKeys [][32]byte
	// seenKeys catches duplicate keys WITHIN one batch: a key reserved by an
	// earlier item of the same batch stays pending until the batch completes,
	// so the later item must fail fast instead of spinning on the conflict.
	seenKeys := make(map[[32]byte]struct{}, len(batch))

	// Phase 1 — reserve idempotency keys. Nothing is submitted yet, so an
	// abort here can roll every reservation back cleanly.
	for i := range batch {
		key := batch[i].IdempotencyKey
		if key != ([32]byte{}) {
			if _, dup := seenKeys[key]; dup {
				outcomes[i] = TransferOutcome{Err: core.ErrIdempotencyConflict{Key: key}}
				continue
			}
			existing, found, err := l.reserveKey(ctx, key)
			if err != nil {
				for _, k := range reservedKeys {
					l.idempKey.Rollback(k)
				}
				if ctx.Err() != nil {
					return outcomes, ctx.Err()
				}
				outcomes[i] = TransferOutcome{Err: err}
				continue
			}
			if found {
				outcomes[i] = TransferOutcome{Transfer: existing}
				continue
			}
			seenKeys[key] = struct{}{}
			reservedKeys = append(reservedKeys, key)
		}
		events = append(events, TransferEvent{Transfer: batch[i]})
		positions = append(positions, i)
	}
	if len(events) == 0 {
		return outcomes, nil
	}

	bs := newBatchState(len(events))
	for j := range events {
		events[j].batch = bs
		events[j].index = j
	}

	// materialize fills positional outcomes from the batch's acks, settling
	// idempotency: successes commit their key, failures roll it back. Shared
	// by the normal and shutdown exit paths.
	materialize := func() {
		for j := range events {
			ack := bs.results[j]
			outcomes[positions[j]] = TransferOutcome{Transfer: ack.Transfer, Err: ack.Err}
			key := ack.Transfer.IdempotencyKey
			if key == ([32]byte{}) {
				continue
			}
			if ack.Err != nil {
				l.idempKey.Rollback(key)
			} else {
				l.idempKey.Commit(key, ack.Transfer)
			}
		}
	}

	// Phase 2 — submit. Retries backpressure on a full ring and honor the
	// LEDGER lifecycle, not the request: once queued, a transfer completes.
	for j := range events {
		for {
			if err := l.rb.Submit(events[j]); err == nil {
				break
			}
			select {
			case <-l.ctx.Done():
				// Loop is going away; complete the never-queued tail here
				// (the loop cannot have seen it) and roll their keys back.
				for k := j; k < len(events); k++ {
					bs.complete(k, TransferAck{Err: core.ErrLedgerClosed{}})
					if key := events[k].Transfer.IdempotencyKey; key != ([32]byte{}) {
						l.idempKey.Rollback(key)
					}
				}
				<-bs.done // submitted prefix: applied or abandoned by the loop
				materialize()
				return outcomes, l.ctx.Err()
			default:
				runtime.Gosched()
			}
		}
	}

	// Phase 3 — one completion notification for the whole batch.
	<-bs.done

	// Phase 4 — materialize positional outcomes and settle idempotency.
	materialize()
	return outcomes, nil
}

func (l *Ledger) reserveKey(ctx context.Context, key [32]byte) (core.Transfer, bool, error) {
	for {
		existing, found, err := l.idempKey.CheckAndReserve(key)
		if err == nil {
			return existing, found, nil
		}
		var conflict core.ErrIdempotencyConflict
		if errors.As(err, &conflict) {
			select {
			case <-ctx.Done():
				return core.Transfer{}, false, ctx.Err()
			default:
				runtime.Gosched()
				continue
			}
		}
		return core.Transfer{}, false, err
	}
}

// CreateAccounts submits a client batch of account creations through one
// WAL append + one sync (one fsync per batch, not per account), returning
// positional outcomes. Duplicates fail per item and never enter the batch.
func (l *Ledger) CreateAccounts(ctx context.Context, accounts []core.Account) ([]AccountOutcome, error) {
	if l.isFollower {
		return nil, core.ErrNotLeader{}
	}
	if len(accounts) == 0 {
		return nil, nil
	}
	if len(accounts) > MaxAccountBatchSize {
		return nil, core.ErrBatchTooLarge{Got: len(accounts), Max: MaxAccountBatchSize}
	}

	results := make([]error, len(accounts))
	ev := AccountCreateEvent{Accounts: accounts, Results: results, Done: make(chan struct{})}

	select {
	case l.loop.accountCreateQueue <- ev:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-ev.Done:
	case <-l.ctx.Done():
		// The loop may exit before servicing the queued batch; treat a
		// shutdown as a failed request (retry against the new leader).
		return nil, core.ErrLedgerClosed{}
	}

	outcomes := make([]AccountOutcome, len(accounts))
	for i := range accounts {
		outcomes[i] = AccountOutcome{Account: accounts[i], Err: results[i]}
	}
	return outcomes, nil
}

func (l *Ledger) GetAccount(ctx context.Context, id [16]byte) (core.Account, error) {
	resChan := AcquireReadResult()
	ev := ReadAccountEvent{ID: id, Result: resChan}

	select {
	case l.loop.readQueue <- ev:
	case <-ctx.Done():
		ReleaseReadResult(resChan) // never queued
		return core.Account{}, ctx.Err()
	}

	select {
	case res := <-resChan:
		ReleaseReadResult(resChan) // drained; safe to recycle
		return res.Account, res.Err
	case <-ctx.Done():
		// Abandoned mid-flight: the loop may still deliver a result, so the
		// channel must not be recycled.
		return core.Account{}, ctx.Err()
	}
}

func (l *Ledger) GetBalance(ctx context.Context, id [16]byte) (core.Uint128, error) {
	acc, err := l.GetAccount(ctx, id)
	if err != nil {
		return core.Uint128{}, err
	}
	bal, err := core.Balance(acc)
	if err != nil {
		observability.InvariantViolations.Inc()
		return core.Uint128{}, err
	}
	return bal, nil
}

func (l *Ledger) TriggerSnapshot(ctx context.Context) (int64, error) {
	if l.loop == nil {
		return 0, nil
	}
	accs, pendings, lsn := l.loop.RequestSnapshotView()
	if lsn <= 0 {
		return 0, nil
	}
	snapPath := filepath.Join(l.snapshotDir, fmt.Sprintf("snapshot-%d.snap", lsn))
	if err := snapshot.Write(snapPath, lsn, accs, pendings); err != nil {
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
			// Replay advances the logical clock and closes expired holds
			// exactly as the live loop did, before applying.
			if t.Timestamp > l.accounts.clock {
				l.accounts.clock = t.Timestamp
			}
			l.accounts.SetClock(l.accounts.clock)
			l.accounts.CloseExpired()
			if err := ApplyTransferState(l.accounts, t); err != nil {
				return err
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
