package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

// C0.13: the snapshot is authoritative. After snapshotting mid-history and
// writing further transfers, a restart must recover the delta above the
// snapshot LSN only, land at the exact WAL head, and preserve balances.
func TestSnapshotMidHistoryRecoveryDelta(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	snapDir := filepath.Join(dir, "snapshots")

	w1, err := wal.Open(walDir, 8<<10)
	if err != nil {
		t.Fatalf("open wal 1: %v", err)
	}
	cfg := engine.DefaultConfig()
	cfg.SnapshotDir = snapDir

	acc1, acc2 := [16]byte{0x01}, [16]byte{0x02}
	currency := [4]byte{'U', 'S', 'D', 0}

	l1, err := engine.NewLedger(cfg, w1)
	if err != nil {
		t.Fatalf("new ledger 1: %v", err)
	}
	ctx := context.Background()

	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc1, Currency: currency, PostedCredits: core.Uint128{Lo: 10_000}}); err != nil {
		t.Fatalf("create acc1: %v", err)
	}
	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc2, Currency: currency}); err != nil {
		t.Fatalf("create acc2: %v", err)
	}

	runTransfers := func(l *engine.Ledger, first, count int) {
		t.Helper()
		for i := first; i < first+count; i++ {
			var trID [16]byte
			trID[0] = byte(i)
			if _, err := l.CreateTransfer(ctx, core.Transfer{
				ID:             trID,
				DebitAccountID: acc1, CreditAccountID: acc2,
				Amount: core.Uint128{Lo: 10},
			}); err != nil {
				t.Fatalf("transfer %d: %v", i, err)
			}
		}
	}

	runTransfers(l1, 1, 40) // pre-snapshot history
	snapLSN, err := l1.TriggerSnapshot(ctx)
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	if snapLSN != l1.CurrentLSN() {
		t.Fatalf("snapshot LSN %d != ledger watermark %d", snapLSN, l1.CurrentLSN())
	}
	runTransfers(l1, 41, 40) // post-snapshot delta

	headLSN := l1.CurrentLSN()
	if headLSN <= snapLSN {
		t.Fatalf("test setup: head %d must exceed snapshot %d", headLSN, snapLSN)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("close ledger 1: %v", err)
	}

	w2, err := wal.Open(walDir, 8<<10)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}
	l2, err := engine.NewLedger(cfg, w2)
	if err != nil {
		t.Fatalf("new ledger 2: %v", err)
	}
	defer l2.Close()

	// The watermark must land exactly on the head — not below it (missed
	// delta) and not re-derived from a full replay.
	if got := l2.CurrentLSN(); got != headLSN {
		t.Fatalf("recovered watermark = %d, want head %d", got, headLSN)
	}

	bal1, err := l2.GetBalance(ctx, acc1)
	if err != nil {
		t.Fatalf("get balance 1: %v", err)
	}
	if bal1.Lo != 10_000-80*10 {
		t.Fatalf("acc1 balance = %d, want %d", bal1.Lo, 10_000-80*10)
	}
	bal2, err := l2.GetBalance(ctx, acc2)
	if err != nil {
		t.Fatalf("get balance 2: %v", err)
	}
	if bal2.Lo != 80*10 {
		t.Fatalf("acc2 balance = %d, want %d", bal2.Lo, 80*10)
	}

	// New writes continue above the recovered watermark.
	runTransfers(l2, 81, 1)
	if got := l2.CurrentLSN(); got != headLSN+2 { // transfer + batch commit
		t.Fatalf("watermark after new write = %d, want %d", got, headLSN+2)
	}
}

// C0.13: snapshot authority must hold even when the surviving WAL is shorter
// than the snapshot's history (e.g. segments lost or manually reclaimed). The
// ledger must come up at the snapshot LSN — never 0 — and new writes must
// continue above it so LSNs are never reused.
func TestSnapshotAuthoritativeWhenWALShorter(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	snapDir := filepath.Join(dir, "snapshots")

	w1, err := wal.Open(walDir, 8<<10)
	if err != nil {
		t.Fatalf("open wal 1: %v", err)
	}
	cfg := engine.DefaultConfig()
	cfg.SnapshotDir = snapDir

	acc1, acc2 := [16]byte{0x01}, [16]byte{0x02}
	currency := [4]byte{'U', 'S', 'D', 0}

	l1, err := engine.NewLedger(cfg, w1)
	if err != nil {
		t.Fatalf("new ledger 1: %v", err)
	}
	ctx := context.Background()

	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc1, Currency: currency, PostedCredits: core.Uint128{Lo: 10_000}}); err != nil {
		t.Fatalf("create acc1: %v", err)
	}
	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc2, Currency: currency}); err != nil {
		t.Fatalf("create acc2: %v", err)
	}
	for i := 1; i <= 40; i++ {
		var trID [16]byte
		trID[0] = byte(i)
		if _, err := l1.CreateTransfer(ctx, core.Transfer{
			ID: trID, DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10},
		}); err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
	}

	snapLSN, err := l1.TriggerSnapshot(ctx)
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("close ledger 1: %v", err)
	}

	// Simulate WAL loss: remove every segment. The snapshot alone must be
	// enough to come back up.
	segs, _ := filepath.Glob(filepath.Join(walDir, "wal-*.seg"))
	if len(segs) == 0 {
		t.Fatalf("test setup: no segments to delete")
	}
	for _, s := range segs {
		if err := os.Remove(s); err != nil {
			t.Fatalf("remove %s: %v", s, err)
		}
	}

	w2, err := wal.Open(walDir, 8<<10)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}
	l2, err := engine.NewLedger(cfg, w2)
	if err != nil {
		t.Fatalf("new ledger 2: %v", err)
	}
	defer l2.Close()

	if got := l2.CurrentLSN(); got != snapLSN {
		t.Fatalf("watermark = %d, want snapshot LSN %d (must not fall back to 0)", got, snapLSN)
	}

	bal1, err := l2.GetBalance(ctx, acc1)
	if err != nil {
		t.Fatalf("get balance 1: %v", err)
	}
	if bal1.Lo != 10_000-40*10 {
		t.Fatalf("acc1 balance = %d, want %d", bal1.Lo, 10_000-40*10)
	}

	// New writes must reuse no LSN at or below the snapshot.
	var trID [16]byte
	trID[0] = 100
	if _, err := l2.CreateTransfer(ctx, core.Transfer{
		ID: trID, DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10},
	}); err != nil {
		t.Fatalf("post-loss transfer: %v", err)
	}
	if got := l2.CurrentLSN(); got <= snapLSN {
		t.Fatalf("watermark after new write = %d, must be > snapshot LSN %d", got, snapLSN)
	}
}
