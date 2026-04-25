package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/observability"
	"aequitas-ledger/internal/wal"
)

// Follower applies the primary's committed WAL stream (C0.8):
//   - group-commit preservation: data records buffer locally and are written
//     with ONE AppendBatch + Sync per primary batch commit — never one fsync
//     per record — and are applied to in-memory state only after they are
//     durable in the follower's own WAL (promotion safety);
//   - exponential backoff with a circuit breaker: repeated failures back off
//     100ms→5s, and after followerBreakerLimit consecutive failures the
//     follower performs a full re-sync (fresh state + WAL reset) instead of
//     resuming a possibly-divergent position;
//   - leader-term fencing: frames from a primary with a term lower than the
//     last accepted term are refused;
//   - on Start the follower first replays its own local WAL, so a restart
//     resumes from the true applied position instead of duplicating history.

// errStalePrimary reports a refused frame/stream from a primary whose term
// is below the follower's last accepted term. It is protective, not a state
// uncertainty: it must NOT trip the circuit breaker (which would wipe valid
// state while the follower waits for a legitimate primary).
var errStalePrimary = errors.New("replication: stale primary term")

const (
	followerBackoffBase    = 100 * time.Millisecond
	followerBackoffMax     = 5 * time.Second
	followerBreakerLimit   = 8 // consecutive failures → full re-sync
	followerFlushThreshold = 4096
	frameHeaderSize        = 21 // [term:8][LSN:8][type:1][len:4]
)

type Follower struct {
	primaryAddr string
	accounts    *engine.AccountManager
	localWAL    *wal.WAL
	lastLSN     int64
	lastTerm    uint64
	termPath    string
	primaryHead atomic.Int64
	connected   atomic.Bool
	failures    atomic.Int64

	// Tunables (set before Start; zero values select production defaults).
	// Exported so tests can exercise the circuit breaker quickly.
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	BreakerLimit int64

	// pending buffers received data records until the primary's commit
	// marker closes the batch (or the size threshold trips). Owned by the
	// syncLoop goroutine.
	pending []wal.Record

	mu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewFollower(primaryAddr string, localWAL *wal.WAL, initialAccounts ...core.Account) *Follower {
	ctx, cancel := context.WithCancel(context.Background())
	accounts := engine.NewAccountManager(1024)
	for _, a := range initialAccounts {
		_ = accounts.Create(a)
	}
	return &Follower{
		primaryAddr: primaryAddr,
		accounts:    accounts,
		localWAL:    localWAL,
		termPath:    followerTermPath(localWAL),
		lastTerm:    loadFollowerTerm(localWAL),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start launches the follower loop. It first replays the local WAL so a
// restarted follower resumes from its true applied position (its own WAL is
// the source of truth for promotion), then connects with exponential backoff.
func (f *Follower) Start() {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		if err := f.recoverLocalState(); err != nil {
			// A follower whose own WAL cannot be replayed must not apply a
			// live stream on top of unknown state.
			fmt.Printf("follower: local WAL recovery failed: %v\n", err)
			return
		}
		for {
			select {
			case <-f.ctx.Done():
				return
			default:
			}
			if err := f.syncLoop(); err != nil {
				if errors.Is(err, errStalePrimary) {
					// Fenced: keep state, back off, wait for a legitimate
					// primary. Never trip the breaker on a refusal.
					observability.ReplicationFollowerConnected.Set(0)
					sleepCtx(f.ctx, 500*time.Millisecond)
					continue
				}
				observability.ReplicationFollowerConnected.Set(0)
				f.failures.Add(1)
				if f.failures.Load() >= f.breakerLimit() {
					if rerr := f.fullResync(); rerr != nil {
						fmt.Printf("follower: full re-sync failed: %v\n", rerr)
						return
					}
					f.failures.Store(0)
				}
				backoff := f.backoffBase() << uint(min64(f.failures.Load()-1, 6))
				if backoff > f.backoffMax() {
					backoff = f.backoffMax()
				}
				sleepCtx(f.ctx, backoff)
			}
		}
	}()
}

func (f *Follower) backoffBase() time.Duration {
	if f.BackoffBase > 0 {
		return f.BackoffBase
	}
	return followerBackoffBase
}

func (f *Follower) backoffMax() time.Duration {
	if f.BackoffMax > 0 {
		return f.BackoffMax
	}
	return followerBackoffMax
}

func (f *Follower) breakerLimit() int64 {
	if f.BreakerLimit > 0 {
		return f.BreakerLimit
	}
	return followerBreakerLimit
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Stop terminates the follower loop.
func (f *Follower) Stop() {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
}

// LastLSN reports the highest primary LSN durably applied by this follower.
func (f *Follower) LastLSN() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastLSN
}

// Lag reports primary-head minus applied LSN (from HEAD announcements);
// negative until the first HEAD frame arrives.
func (f *Follower) Lag() int64 {
	return f.primaryHead.Load() - f.LastLSN()
}

// Connected reports whether a validated replication stream is open.
func (f *Follower) Connected() bool {
	return f.connected.Load()
}

func (f *Follower) GetAccount(id [16]byte) (core.Account, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	acc, err := f.accounts.Get(id)
	if err != nil {
		return core.Account{}, err
	}
	return *acc, nil
}

func (f *Follower) Accounts() []core.Account {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.accounts.Snapshot()
}

// recoverLocalState replays the follower's own WAL into in-memory state and
// positions lastLSN at the primary's applied watermark. The follower does not
// persist primary commit markers, so the watermark is derived from the data
// records: primary LSNs are contiguous and every commit immediately follows
// its batch's last record, making the applied commit LSN = maxDataLSN + 1
// (also correct for size-threshold flushes that landed before their commit).
func (f *Follower) recoverLocalState() error {
	var maxDataLSN int64
	err := f.localWAL.Recover(func(r wal.Record) error {
		if r.Type == wal.RecordTypeBatchCommit || r.Type == wal.RecordTypeHead {
			return nil
		}
		if int64(r.LSN) > maxDataLSN {
			maxDataLSN = int64(r.LSN)
		}
		return f.applyRecord(r)
	})
	if err != nil {
		return err
	}
	applied := maxDataLSN
	if applied > 0 {
		applied++ // the commit LSN of the last streamed batch
	}
	f.mu.Lock()
	f.lastLSN = applied
	f.mu.Unlock()
	observability.ReplicationFollowerLastLSN.Set(float64(applied))
	return nil
}

// fullResync implements the circuit breaker's hard reset: drop in-memory
// state, wipe the local WAL, and let the next connection stream from LSN 1.
func (f *Follower) fullResync() error {
	f.mu.Lock()
	f.accounts = engine.NewAccountManager(1024)
	f.lastLSN = 0
	f.mu.Unlock()
	f.pending = f.pending[:0]
	observability.ReplicationFollowerLastLSN.Set(0)
	return f.localWAL.Reset()
}

func (f *Follower) syncLoop() error {
	conn, err := net.Dial("tcp", f.primaryAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Handshake: resume LSN out, leader term in.
	f.mu.RLock()
	resume := f.lastLSN + 1
	f.mu.RUnlock()
	if _, err := conn.Write(beUint64(uint64(resume))); err != nil {
		return err
	}
	var termBuf [8]byte
	if _, err := io.ReadFull(conn, termBuf[:]); err != nil {
		return err
	}
	term := binary.BigEndian.Uint64(termBuf[:])
	if f.lastTerm != 0 && term < f.lastTerm {
		return fmt.Errorf("stale primary: stream term %d below last accepted %d (possible split-brain): %w", term, f.lastTerm, errStalePrimary)
	}
	if term > f.lastTerm {
		f.lastTerm = term
		if err := storeFollowerTerm(f.localWAL, f.lastTerm); err != nil {
			return fmt.Errorf("persist follower term: %w", err)
		}
	}

	// Handshake validated: the breaker counts connection failures, not idle
	// time on a healthy stream.
	f.failures.Store(0)
	f.connected.Store(true)
	observability.ReplicationFollowerConnected.Set(1)
	observability.ReplicationFollowerReconnects.Inc()

	header := make([]byte, frameHeaderSize)
	for {
		select {
		case <-f.ctx.Done():
			return nil
		default:
		}

		if _, err := io.ReadFull(conn, header); err != nil {
			return err
		}
		frameTerm := binary.BigEndian.Uint64(header[0:8])
		lsn := int64(binary.BigEndian.Uint64(header[8:16]))
		recType := wal.RecordType(header[16])
		payloadLen := binary.BigEndian.Uint32(header[17:21])

		body := make([]byte, payloadLen+4) // payload + CRC32
		if _, err := io.ReadFull(conn, body); err != nil {
			return err
		}
		payload := body[:payloadLen]
		expectedCRC := binary.BigEndian.Uint32(body[payloadLen:])
		frame := make([]byte, frameHeaderSize+payloadLen)
		copy(frame, header)
		copy(frame[frameHeaderSize:], payload)
		if crc32.ChecksumIEEE(frame) != expectedCRC {
			return fmt.Errorf("replication stream CRC mismatch at LSN %d", lsn)
		}
		if frameTerm < f.lastTerm {
			return fmt.Errorf("stale primary frame: term %d below last accepted %d: %w", frameTerm, f.lastTerm, errStalePrimary)
		}

		rec := wal.Record{LSN: uint64(lsn), Type: recType, Payload: payload}
		switch rec.Type {
		case wal.RecordTypeHead:
			if len(payload) >= 8 {
				f.primaryHead.Store(int64(binary.BigEndian.Uint64(payload[:8])))
				if lag := f.Lag(); lag >= 0 {
					observability.ReplicationFollowerLag.Set(float64(lag))
				}
			}
		case wal.RecordTypeBatchCommit:
			// The primary's commit closes the batch: persist the buffered
			// records with one group commit, then apply them to state.
			if err := f.flush(); err != nil {
				return fmt.Errorf("follower flush at LSN %d: %w", lsn, err)
			}
			f.mu.Lock()
			if lsn > f.lastLSN {
				f.lastLSN = lsn
			}
			f.mu.Unlock()
			observability.ReplicationFollowerLastLSN.Set(float64(lsn))
		default:
			// Data record: skip duplicates (a reconnect re-sends from
			// lastLSN+1), buffer for the next group commit.
			f.mu.RLock()
			dup := lsn <= f.lastLSN
			f.mu.RUnlock()
			if dup {
				continue
			}
			f.pending = append(f.pending, rec)
			if len(f.pending) >= followerFlushThreshold {
				if err := f.flush(); err != nil {
					return fmt.Errorf("follower flush at LSN %d: %w", lsn, err)
				}
			}
		}
	}
}

// flush writes the buffered records with one AppendBatch + Sync (group
// commit preserved on the follower) and only then applies them to state —
// the follower WAL is always at least as advanced as its in-memory view, so
// promotion replays a complete history. Any failure here leaves the
// follower's position uncertain: the caller propagates it and the circuit
// breaker performs a full re-sync rather than resuming divergent state.
func (f *Follower) flush() error {
	if len(f.pending) == 0 {
		return nil
	}
	batch := f.pending
	f.pending = f.pending[:0]

	if _, err := f.localWAL.AppendBatch(batch); err != nil {
		return err
	}
	if err := f.localWAL.Sync(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rec := range batch {
		if err := f.applyRecord(rec); err != nil {
			return err
		}
	}
	return nil
}

func (f *Follower) applyRecord(r wal.Record) error {
	switch r.Type {
	case wal.RecordTypeAccount:
		acc, err := engine.DecodeAccountPayload(r.Payload)
		if err != nil {
			return err
		}
		if _, err := f.accounts.Get(acc.ID); err == nil {
			return nil // already present
		}
		return f.accounts.Create(acc)
	case wal.RecordTypeTransfer:
		t, err := engine.DecodeTransferPayload(r.Payload)
		if err != nil {
			return err
		}
		if _, err := f.accounts.Get(t.DebitAccountID); err != nil {
			return fmt.Errorf("follower missing debit account %v: %w", t.DebitAccountID, err)
		}
		if _, err := f.accounts.Get(t.CreditAccountID); err != nil {
			return fmt.Errorf("follower missing credit account %v: %w", t.CreditAccountID, err)
		}
		if err := f.accounts.ApplyDebit(t.DebitAccountID, t.Amount); err != nil {
			return err
		}
		return f.accounts.ApplyCredit(t.CreditAccountID, t.Amount)
	default:
		return nil
	}
}
