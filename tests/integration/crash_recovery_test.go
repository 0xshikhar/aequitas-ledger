package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	cfg := engine.DefaultConfig()
	cfg.RingBufferSize = 1 << 12
	cfg.MaxBatchSize = 64
	cfg.BatchTimeout = 500 * time.Microsecond

	// 1. Create accounts with initial balances
	acc1 := core.Account{ID: mkID(1), Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(1_000_000)}
	acc2 := core.Account{ID: mkID(2), Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(500_000)}
	acc3 := core.Account{ID: mkID(3), Currency: [4]byte{'U', 'S', 'D', 'C'}, PostedCredits: core.FromUint64(250_000)}
	cfg.InitialAccounts = []core.Account{acc1, acc2, acc3}

	w1, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("failed to open initial WAL: %v", err)
	}

	ledger1, err := engine.NewLedger(cfg, w1)
	if err != nil {
		t.Fatalf("failed to start initial ledger: %v", err)
	}

	// Track expected account balances locally as ground truth
	expectedBalances := map[[16]byte]core.Uint128{
		acc1.ID: core.FromUint64(1_000_000),
		acc2.ID: core.FromUint64(500_000),
		acc3.ID: core.FromUint64(250_000),
	}

	// 2. Run 10 batches of transfers
	ctx := context.Background()
	transferCount := 0

	for batch := 0; batch < 10; batch++ {
		for i := 0; i < 20; i++ {
			transferCount++
			var key [32]byte
			copy(key[:], fmt.Sprintf("batch-%d-tx-%d", batch, i))

			var fromID, toID [16]byte
			var amount uint64 = uint64(100 + i*10)

			if i%2 == 0 {
				fromID = acc1.ID
				toID = acc2.ID
			} else {
				fromID = acc2.ID
				toID = acc3.ID
			}

			tr := core.Transfer{
				ID:              mkID(uint64(transferCount)),
				DebitAccountID:  fromID,
				CreditAccountID: toID,
				Amount:          core.FromUint64(amount),
				IdempotencyKey:  key,
			}

			_, err := ledger1.CreateTransfer(ctx, tr)
			if err != nil {
				t.Fatalf("CreateTransfer failed during pre-crash execution: %v", err)
			}

			// Update ground truth expectations
			var errSub, errAdd error
			expectedBalances[fromID], errSub = core.Sub(expectedBalances[fromID], core.FromUint64(amount))
			if errSub != nil {
				t.Fatalf("unexpected sub error: %v", errSub)
			}
			expectedBalances[toID], errAdd = core.Add(expectedBalances[toID], core.FromUint64(amount))
			if errAdd != nil {
				t.Fatalf("unexpected add error: %v", errAdd)
			}
		}
	}

	// Verify ground truth before simulated crash
	for _, id := range [][16]byte{acc1.ID, acc2.ID, acc3.ID} {
		actual, err := ledger1.GetBalance(ctx, id)
		if err != nil {
			t.Fatalf("GetBalance failed pre-crash: %v", err)
		}
		if core.Cmp(actual, expectedBalances[id]) != 0 {
			t.Fatalf("pre-crash balance mismatch for account %v: got %s, expected %s",
				id, core.String(actual), core.String(expectedBalances[id]))
		}
	}

	// 3. Simulate ungraceful crash:
	// Kill the engine event loop context and cancel without calling ledger1.Close() or wal1.Close() cleanly.
	// We intentionally do NOT call w1.Close() to simulate an abrupt process termination mid-flight.

	// 4. Restart: open new WAL and Ledger instance over the same WAL directory to trigger recover.
	w2, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("failed to open WAL post-crash: %v", err)
	}

	ledger2, err := engine.NewLedger(cfg, w2)
	if err != nil {
		t.Fatalf("failed to recover ledger post-crash: %v", err)
	}
	defer func() { _ = ledger2.Close() }()

	// 5. Assert recovered balances == ground truth
	var sumRecoveredBalance core.Uint128
	var sumInitialBalance core.Uint128

	for _, initialAcc := range cfg.InitialAccounts {
		var errAdd error
		sumInitialBalance, errAdd = core.Add(sumInitialBalance, core.Balance(initialAcc))
		if errAdd != nil {
			t.Fatalf("unexpected sumInitialBalance add error: %v", errAdd)
		}

		recoveredAcc, err := ledger2.GetAccount(ctx, initialAcc.ID)
		if err != nil {
			t.Fatalf("failed to get recovered account %v: %v", initialAcc.ID, err)
		}
		recBalance := core.Balance(recoveredAcc)
		sumRecoveredBalance, errAdd = core.Add(sumRecoveredBalance, recBalance)
		if errAdd != nil {
			t.Fatalf("unexpected sumRecoveredBalance add error: %v", errAdd)
		}

		expected := expectedBalances[initialAcc.ID]
		if core.Cmp(recBalance, expected) != 0 {
			t.Fatalf("recovered balance mismatch for account %v: got %s, expected ground truth %s",
				initialAcc.ID, core.String(recBalance), core.String(expected))
		}
	}

	// 6. Assert double-entry invariant holds across all accounts:
	// Total initial money in system == total recovered money in system
	if core.Cmp(sumRecoveredBalance, sumInitialBalance) != 0 {
		t.Fatalf("double-entry invariant violated! total system balance before = %s, after recovery = %s",
			core.String(sumInitialBalance), core.String(sumRecoveredBalance))
	}

	_ = os.RemoveAll(dir)
}
