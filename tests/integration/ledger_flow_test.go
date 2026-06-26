package integration

import (
	"context"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestLedgerCreateTransferFullPath(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	cfg := engine.DefaultConfig()
	cfg.RingBufferSize = 1 << 12
	cfg.MaxBatchSize = 128
	cfg.BatchTimeout = 200 * time.Microsecond

	debit := core.Account{ID: mkID(100), Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(1000)}
	credit := core.Account{ID: mkID(200), Currency: [4]byte{'U', 'S', 'D', 'C'}}
	cfg.InitialAccounts = []core.Account{debit, credit}

	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ledger.Close() }()

	var key [32]byte
	copy(key[:], []byte("flow-1"))

	tr := core.Transfer{
		ID:              mkID(300),
		DebitAccountID:  debit.ID,
		CreditAccountID: credit.ID,
		Amount:          core.FromUint64(111),
		IdempotencyKey:  key,
	}

	got, err := ledger.CreateTransfer(context.Background(), tr)
	if err != nil {
		t.Fatalf("CreateTransfer failed: %v", err)
	}
	if got.ID != tr.ID {
		t.Fatalf("unexpected transfer response id")
	}

	accDebit, err := ledger.GetAccount(context.Background(), debit.ID)
	if err != nil {
		t.Fatal(err)
	}
	accCredit, err := ledger.GetAccount(context.Background(), credit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if b := core.String(balanceOf(t, accDebit)); b != "889" {
		t.Fatalf("unexpected debit balance: %s", b)
	}
	if b := core.String(balanceOf(t, accCredit)); b != "111" {
		t.Fatalf("unexpected credit balance: %s", b)
	}
}
