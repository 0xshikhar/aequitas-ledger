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

func TestReplicationPrimaryToFollower(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aequitas-repl-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	primaryWAL, err := wal.Open(filepath.Join(tmpDir, "primary-wal"), 64<<20)
	if err != nil {
		t.Fatalf("failed to open primary wal: %v", err)
	}

	acc1 := core.Account{ID: [16]byte{1}, Currency: [4]byte{'U', 'S', 'D'}, PostedCredits: core.FromUint64(10000)}
	acc2 := core.Account{ID: [16]byte{2}, Currency: [4]byte{'U', 'S', 'D'}, PostedCredits: core.FromUint64(0)}

	cfg := engine.DefaultConfig()
	cfg.InitialAccounts = []core.Account{acc1, acc2}

	primaryLedger, err := engine.NewLedger(cfg, primaryWAL)
	if err != nil {
		t.Fatalf("failed to create primary ledger: %v", err)
	}
	defer primaryLedger.Close()

	replAddr := "127.0.0.1:17001"
	replServer := replication.NewServer(replAddr, primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("failed to start replication server: %v", err)
	}
	defer replServer.Stop()

	// 1. Submit transfers on Primary
	ctx := context.Background()
	tr1 := core.Transfer{
		ID:              [16]byte{101},
		DebitAccountID:  acc1.ID,
		CreditAccountID: acc2.ID,
		Amount:          core.FromUint64(300),
		IdempotencyKey:  [32]byte{1},
	}
	if _, err := primaryLedger.CreateTransfer(ctx, tr1); err != nil {
		t.Fatalf("primary transfer 1 failed: %v", err)
	}

	// 2. Start Follower replica
	followerWAL, _ := wal.Open(filepath.Join(tmpDir, "follower-wal"), 64<<20)
	follower := replication.NewFollower(replAddr, followerWAL, acc1, acc2)
	follower.Start()
	defer follower.Stop()

	// Wait for follower catchup
	time.Sleep(200 * time.Millisecond)

	// 3. Verify Follower state
	followerAcc1, err := follower.GetAccount(acc1.ID)
	if err != nil {
		t.Fatalf("failed to get account 1 on follower: %v", err)
	}
	bal1 := balanceOf(t, followerAcc1)
	if core.String(bal1) != "9700" {
		t.Errorf("expected follower account 1 balance 9700, got %s", core.String(bal1))
	}

	followerAcc2, err := follower.GetAccount(acc2.ID)
	if err != nil {
		t.Fatalf("failed to get account 2 on follower: %v", err)
	}
	bal2 := balanceOf(t, followerAcc2)
	if core.String(bal2) != "300" {
		t.Errorf("expected follower account 2 balance 300, got %s", core.String(bal2))
	}

	// 4. Verify Net Balance Conservation Invariant on Follower
	followerAccs := follower.Accounts()
	var totalSystemBalance core.Uint128
	for _, a := range followerAccs {
		totalSystemBalance, _ = core.Add(totalSystemBalance, balanceOf(t, a))
	}
	if core.String(totalSystemBalance) != "10000" {
		t.Errorf("follower balance conservation violated! system balance=%s, expected=10000", core.String(totalSystemBalance))
	}
}
