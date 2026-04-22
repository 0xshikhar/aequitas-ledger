package integration

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestConcurrentReadWriteRace(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	acc1ID := [16]byte{0x01}
	acc2ID := [16]byte{0x02}
	currency := [4]byte{'U', 'S', 'D', 0}

	cfg := engine.DefaultConfig()
	cfg.InitialAccounts = []core.Account{
		{ID: acc1ID, Currency: currency, PostedCredits: core.Uint128{Lo: 1_000_000}},
		{ID: acc2ID, Currency: currency, PostedCredits: core.Uint128{Lo: 1_000_000}},
	}

	l, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			var trID [16]byte
			trID[0] = byte(i)
			trID[1] = byte(i >> 8)
			_, _ = l.CreateTransfer(ctx, core.Transfer{
				ID:              trID,
				DebitAccountID:  acc1ID,
				CreditAccountID: acc2ID,
				Amount:          core.Uint128{Lo: 1},
			})
		}
	}()

	// Reader goroutines
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, _ = l.GetAccount(ctx, acc1ID)
				_, _ = l.GetBalance(ctx, acc2ID)
			}
		}()
	}

	// Account creator goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			var newID [16]byte
			newID[15] = byte(i + 10)
			acc := core.Account{ID: newID, Currency: currency}
			_, _ = l.CreateAccount(ctx, acc)
		}
	}()

	wg.Wait()
}
