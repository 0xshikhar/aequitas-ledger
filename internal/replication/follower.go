package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

type Follower struct {
	primaryAddr string
	accounts    *engine.AccountManager
	localWAL    *wal.WAL
	lastLSN     int64
	mu          sync.RWMutex

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
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (f *Follower) Start() {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			select {
			case <-f.ctx.Done():
				return
			default:
				if err := f.syncLoop(); err != nil {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()
}

func (f *Follower) Stop() {
	if f.cancel != nil {
		f.cancel()
	}
	f.wg.Wait()
}

func (f *Follower) LastLSN() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastLSN
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

func (f *Follower) syncLoop() error {
	conn, err := net.Dial("tcp", f.primaryAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send current LSN request (8 bytes)
	f.mu.RLock()
	currentLSN := f.lastLSN
	f.mu.RUnlock()

	var reqBuf [8]byte
	binary.BigEndian.PutUint64(reqBuf[:], uint64(currentLSN+1))
	if _, err := conn.Write(reqBuf[:]); err != nil {
		return err
	}

	headerBuf := make([]byte, 13) // [LSN:8B] [Type:1B] [Len:4B]
	for {
		select {
		case <-f.ctx.Done():
			return nil
		default:
		}

		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			return err
		}

		lsn := int64(binary.BigEndian.Uint64(headerBuf[0:8]))
		recType := wal.RecordType(headerBuf[8])
		payloadLen := binary.BigEndian.Uint32(headerBuf[9:13])

		bodyBuf := make([]byte, payloadLen+4) // payload + 4B CRC32
		if _, err := io.ReadFull(conn, bodyBuf); err != nil {
			return err
		}

		payload := bodyBuf[:payloadLen]
		expectedCRC := binary.BigEndian.Uint32(bodyBuf[payloadLen:])

		// Construct full header+payload frame for CRC validation
		frame := make([]byte, 13+payloadLen)
		copy(frame[0:13], headerBuf)
		copy(frame[13:], payload)
		actualCRC := crc32.ChecksumIEEE(frame)

		if expectedCRC != actualCRC {
			return fmt.Errorf("replication stream CRC mismatch: got %d want %d", actualCRC, expectedCRC)
		}

		rec := wal.Record{LSN: uint64(lsn), Type: recType, Payload: payload}

		// Persist to local follower WAL first for promotion safety
		if f.localWAL != nil {
			if _, err := f.localWAL.AppendBatch([]wal.Record{rec}); err != nil {
				return fmt.Errorf("follower local wal append failed: %w", err)
			}
			if err := f.localWAL.Sync(); err != nil {
				return fmt.Errorf("follower local wal sync failed: %w", err)
			}
		}

		f.mu.Lock()
		if lsn > f.lastLSN {
			if err := f.applyRecord(rec); err != nil {
				f.mu.Unlock()
				return fmt.Errorf("follower apply failed at LSN %d: %w", lsn, err)
			}
			f.lastLSN = lsn
		}
		f.mu.Unlock()
	}
}

func (f *Follower) applyRecord(r wal.Record) error {
	switch r.Type {
	case wal.RecordTypeAccount:
		acc, err := engine.DecodeAccountPayload(r.Payload)
		if err != nil {
			return err
		}
		if _, err := f.accounts.Get(acc.ID); err == nil {
			return nil // Already present
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
