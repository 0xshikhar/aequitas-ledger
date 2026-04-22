package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}

		if recType == wal.RecordTypeTransfer {
			t, err := decodeTransferPayload(payload)
			if err == nil {
				f.mu.Lock()
				if lsn > f.lastLSN {
					f.applyTransfer(t)
					f.lastLSN = lsn
				}
				f.mu.Unlock()
			}
		}
	}
}

func (f *Follower) applyTransfer(t core.Transfer) {
	debitAcc, err := f.accounts.Get(t.DebitAccountID)
	if err != nil {
		// Auto-seed missing account for follower readiness if needed
		_ = f.accounts.Create(core.Account{ID: t.DebitAccountID})
	}
	creditAcc, err := f.accounts.Get(t.CreditAccountID)
	if err != nil {
		_ = f.accounts.Create(core.Account{ID: t.CreditAccountID})
	}
	_ = debitAcc
	_ = creditAcc

	f.accounts.ApplyDebit(t.DebitAccountID, t.Amount)
	_ = f.accounts.ApplyCredit(t.CreditAccountID, t.Amount)
}

func decodeTransferPayload(payload []byte) (core.Transfer, error) {
	if len(payload) < 16+16+16+16+32+8 {
		return core.Transfer{}, fmt.Errorf("transfer payload too short: %d", len(payload))
	}

	var tr core.Transfer
	copy(tr.ID[:], payload[0:16])
	copy(tr.DebitAccountID[:], payload[16:32])
	copy(tr.CreditAccountID[:], payload[32:48])

	tr.Amount = core.Uint128{
		Hi: binary.BigEndian.Uint64(payload[48:56]),
		Lo: binary.BigEndian.Uint64(payload[56:64]),
	}

	if len(payload) >= 96 {
		copy(tr.IdempotencyKey[:], payload[64:96])
	}
	if len(payload) >= 104 {
		tr.Timestamp = int64(binary.BigEndian.Uint64(payload[96:104]))
	}

	return tr, nil
}

type dummyWriter struct{}

func (dummyWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func encodeTransfer(t core.Transfer) []byte {
	buf := new(bytes.Buffer)
	buf.Write(t.ID[:])
	buf.Write(t.DebitAccountID[:])
	buf.Write(t.CreditAccountID[:])
	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], t.Amount.Hi)
	buf.Write(b8[:])
	binary.BigEndian.PutUint64(b8[:], t.Amount.Lo)
	buf.Write(b8[:])
	buf.Write(t.IdempotencyKey[:])
	binary.BigEndian.PutUint64(b8[:], uint64(t.Timestamp))
	return buf.Bytes()
}
