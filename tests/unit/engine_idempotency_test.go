package unit

import (
	"context"
	"sync"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestIdempotencyConcurrentSameKeySameResult(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	cfg := engine.DefaultConfig()
	cfg.RingBufferSize = 1 << 12
	cfg.MaxBatchSize = 256
	cfg.BatchTimeout = 200 * time.Microsecond

	debit := core.Account{ID: id16(1), Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(10)}
	credit := core.Account{ID: id16(2), Currency: [4]byte{'U', 'S', 'D', 'C'}}
	cfg.InitialAccounts = []core.Account{debit, credit}

	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()

	var key [32]byte
	copy(key[:], []byte("same-idempotency-key"))
	tr := core.Transfer{
		ID:              id16(1001),
		DebitAccountID:  debit.ID,
		CreditAccountID: credit.ID,
		Amount:          core.FromUint64(3),
		IdempotencyKey:  key,
	}

	type result struct {
		tr  core.Transfer
		err error
	}
	out := make([]result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			out[i].tr, out[i].err = ledger.CreateTransfer(context.Background(), tr)
		}()
	}
	wg.Wait()

	if out[0].err != nil || out[1].err != nil {
		t.Fatalf("expected both calls to succeed, got err0=%v err1=%v", out[0].err, out[1].err)
	}
	if out[0].tr.ID != out[1].tr.ID {
		t.Fatalf("idempotent calls returned different transfers")
	}

	accDebit, err := ledger.GetAccount(context.Background(), debit.ID)
	if err != nil {
		t.Fatal(err)
	}
	accCredit, err := ledger.GetAccount(context.Background(), credit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := core.String(core.Balance(accDebit)); got != "7" {
		t.Fatalf("unexpected debit account balance: %s", got)
	}
	if got := core.String(core.Balance(accCredit)); got != "3" {
		t.Fatalf("unexpected credit account balance: %s", got)
	}
}
