package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestEventLoopConcurrentTransfersInvariant(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	am := engine.NewAccountManager(2)
	debitID := mkID(1)
	creditID := mkID(2)
	if err := am.Create(core.Account{ID: debitID, Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(20_000)}); err != nil {
		t.Fatal(err)
	}
	if err := am.Create(core.Account{ID: creditID, Currency: [4]byte{'U', 'S', 'D', 'C'}}); err != nil {
		t.Fatal(err)
	}

	rb := engine.NewRingBuffer(1 << 15)
	batcher := engine.NewBatcher(rb, 1024, 500*time.Microsecond)
	loop := engine.NewEventLoop(batcher, am, w)

	initial := am.Snapshot()
	initialDebits := core.Uint128{}
	initialCredits := core.Uint128{}
	for _, a := range initial {
		var e error
		initialDebits, e = core.Add(initialDebits, a.PostedDebits)
		if e != nil {
			t.Fatalf("initial debits overflow: %v", e)
		}
		initialCredits, e = core.Add(initialCredits, a.PostedCredits)
		if e != nil {
			t.Fatalf("initial credits overflow: %v", e)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var loopWG sync.WaitGroup
	loopWG.Add(1)
	go func() {
		defer loopWG.Done()
		loop.Run(ctx)
	}()

	const goroutines = 50
	const each = 200
	const total = goroutines * each

	results := make(chan error, total)
	var submitWG sync.WaitGroup
	submitWG.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer submitWG.Done()
			for i := 0; i < each; i++ {
				ev := engine.NewTransferEvent(core.Transfer{
					ID:              mkID(uint64(g*each + i + 1_000)),
					DebitAccountID:  debitID,
					CreditAccountID: creditID,
					Amount:          core.FromUint64(1),
				})
				for {
					if err := rb.Submit(ev); err == nil {
						break
					}
					time.Sleep(time.Microsecond)
				}
				results <- (<-ev.Result).Err
			}
		}()
	}
	submitWG.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("transfer failed: %v", err)
		}
	}

	cancel()
	loopWG.Wait()

	debitAcc, _ := am.Get(debitID)
	creditAcc, _ := am.Get(creditID)
	if got := core.String(core.Balance(*debitAcc)); got != "10000" {
		t.Fatalf("unexpected debit balance: %s", got)
	}
	if got := core.String(core.Balance(*creditAcc)); got != "10000" {
		t.Fatalf("unexpected credit balance: %s", got)
	}

	snap := am.Snapshot()
	totalDebits := core.Uint128{}
	totalCredits := core.Uint128{}
	for _, a := range snap {
		var e error
		totalDebits, e = core.Add(totalDebits, a.PostedDebits)
		if e != nil {
			t.Fatalf("total debits overflow: %v", e)
		}
		totalCredits, e = core.Add(totalCredits, a.PostedCredits)
		if e != nil {
			t.Fatalf("total credits overflow: %v", e)
		}
	}
	deltaDebits, err := core.Sub(totalDebits, initialDebits)
	if err != nil {
		t.Fatalf("delta debits underflow: %v", err)
	}
	deltaCredits, err := core.Sub(totalCredits, initialCredits)
	if err != nil {
		t.Fatalf("delta credits underflow: %v", err)
	}
	if core.Cmp(deltaDebits, deltaCredits) != 0 {
		t.Fatalf("double-entry invariant violated on transfer delta: debits=%s credits=%s", core.String(deltaDebits), core.String(deltaCredits))
	}
}
