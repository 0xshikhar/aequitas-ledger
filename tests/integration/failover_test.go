package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/replication"
	"aequitas-ledger/internal/wal"
)

func TestReplicaFailoverPromotion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aequitas-failover-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	primaryWAL, err := wal.Open(filepath.Join(tmpDir, "primary-wal"), 64<<20)
	if err != nil {
		t.Fatalf("failed to open primary wal: %v", err)
	}

	acc1 := core.Account{ID: [16]byte{1}, Currency: [4]byte{'U', 'S', 'D'}, PostedCredits: core.FromUint64(5000)}
	acc2 := core.Account{ID: [16]byte{2}, Currency: [4]byte{'U', 'S', 'D'}, PostedCredits: core.FromUint64(0)}

	cfg := engine.DefaultConfig()
	cfg.InitialAccounts = []core.Account{acc1, acc2}

	primaryLedger, err := engine.NewLedger(cfg, primaryWAL)
	if err != nil {
		t.Fatalf("failed to create primary ledger: %v", err)
	}

	replAddr := "127.0.0.1:17002"
	replServer := replication.NewServer(replAddr, primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("failed to start replication server: %v", err)
	}

	// 1. Submit transfer 1 on Primary (1,000 USD)
	ctx := context.Background()
	tr1 := core.Transfer{
		ID:              [16]byte{101},
		DebitAccountID:  acc1.ID,
		CreditAccountID: acc2.ID,
		Amount:          core.FromUint64(1000),
		IdempotencyKey:  [32]byte{1},
	}
	if _, err := primaryLedger.CreateTransfer(ctx, tr1); err != nil {
		t.Fatalf("primary transfer 1 failed: %v", err)
	}

	// 2. Start Follower replica and wait for catchup
	followerWAL, _ := wal.Open(filepath.Join(tmpDir, "follower-wal"), 64<<20)
	follower := replication.NewFollower(replAddr, followerWAL, acc1, acc2)
	follower.Start()

	time.Sleep(200 * time.Millisecond)

	// 3. Simulate Primary failure
	replServer.Stop()
	_ = primaryLedger.Close()

	// 4. Promote Follower to Primary
	promotedPrimary, err := follower.PromoteToPrimary(cfg)
	if err != nil {
		t.Fatalf("failed to promote follower to primary: %v", err)
	}
	defer promotedPrimary.Close()

	// 5. Submit transfer 2 on Promoted Primary (500 USD)
	tr2 := core.Transfer{
		ID:              [16]byte{102},
		DebitAccountID:  acc1.ID,
		CreditAccountID: acc2.ID,
		Amount:          core.FromUint64(500),
		IdempotencyKey:  [32]byte{2},
	}
	if _, err := promotedPrimary.CreateTransfer(ctx, tr2); err != nil {
		t.Fatalf("promoted primary transfer 2 failed: %v", err)
	}

	// 6. Verify Final Account Balances on Promoted Primary
	// Acc 1: 5000 - 1000 - 500 = 3500
	acc1State, err := promotedPrimary.GetAccount(ctx, acc1.ID)
	if err != nil {
		t.Fatalf("failed to get account 1: %v", err)
	}
	if core.String(balanceOf(t, acc1State)) != "3500" {
		t.Errorf("expected account 1 balance '3500', got '%s'", core.String(balanceOf(t, acc1State)))
	}

	// Acc 2: 0 + 1000 + 500 = 1500
	acc2State, err := promotedPrimary.GetAccount(ctx, acc2.ID)
	if err != nil {
		t.Fatalf("failed to get account 2: %v", err)
	}
	if core.String(balanceOf(t, acc2State)) != "1500" {
		t.Errorf("expected account 2 balance '1500', got '%s'", core.String(balanceOf(t, acc2State)))
	}
}
