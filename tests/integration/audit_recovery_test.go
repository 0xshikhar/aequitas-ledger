package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"aequitas-ledger/internal/audit"
	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

// T3.4 end-to-end: a WAL produced by the real engine — accounts, keyed
// transfers, multiple batches, snapshot truncation — must audit clean.
func TestAuditEngineWALPasses(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	w, err := wal.Open(walDir, 1<<15)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	cfg := engine.DefaultConfig()
	cfg.SnapshotDir = filepath.Join(dir, "snapshots")
	l, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}

	ctx := context.Background()
	acc1, acc2 := mkID(1), mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}
	if _, err := l.CreateAccounts(ctx, []core.Account{
		{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 100_000}},
		{ID: acc2, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}
	for b := 0; b < 5; b++ {
		batch := make([]core.Transfer, 40)
		for i := range batch {
			n := uint64(b*40 + i + 1)
			var key [32]byte
			key[0] = byte(n)
			batch[i] = core.Transfer{
				ID: mkID(n), DebitAccountID: acc1, CreditAccountID: acc2,
				Amount: core.Uint128{Lo: 1}, IdempotencyKey: key,
			}
		}
		if _, err := l.CreateTransfers(ctx, batch); err != nil {
			t.Fatalf("transfer batch: %v", err)
		}
	}
	if _, err := l.TriggerSnapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rep, err := audit.Audit(walDir)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if !rep.Valid() {
		t.Fatalf("engine-produced WAL has violations: %v", rep.Violations)
	}
	if rep.Transfers != 200 {
		t.Fatalf("transfers = %d, want 200", rep.Transfers)
	}
	t.Logf("audit clean: %d records, %d batches, %d segments", rep.Records, rep.Batches, rep.Segments)
}

// S2.4 evidence: recovery time from a full WAL replay versus a
// snapshot-served restart, measured on identical history.
func TestRecoveryTimeFullReplayVsSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("recovery timing needs the full workload")
	}
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	snapDir := filepath.Join(dir, "snapshots")

	build := func() *engine.Ledger {
		t.Helper()
		w, err := wal.Open(walDir, 1<<20)
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}
		cfg := engine.DefaultConfig()
		cfg.SnapshotDir = snapDir
		l, err := engine.NewLedger(cfg, w)
		if err != nil {
			t.Fatalf("new ledger: %v", err)
		}
		return l
	}

	l1 := build()
	ctx := context.Background()
	acc1, acc2 := mkID(1), mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}
	if _, err := l1.CreateAccounts(ctx, []core.Account{
		{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 10_000_000}},
		{ID: acc2, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}
	const batches, perBatch = 20, 8192
	for b := 0; b < batches; b++ {
		batch := make([]core.Transfer, perBatch)
		for i := range batch {
			n := uint64(b*perBatch + i + 1)
			batch[i] = core.Transfer{ID: mkID(n), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 1}}
		}
		if _, err := l1.CreateTransfers(ctx, batch); err != nil {
			t.Fatalf("transfer batch: %v", err)
		}
	}
	headLSN := l1.CurrentLSN()
	snapLSN, err := l1.TriggerSnapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Measure min-of-5 per path: page-cache warmth dominates a single run,
	// so the minimum is the honest comparison.
	measure := func(ignoreSnapshot bool) time.Duration {
		t.Helper()
		best := time.Duration(1 << 62)
		for i := 0; i < 5; i++ {
			cfg := engine.DefaultConfig()
			cfg.SnapshotDir = snapDir
			if ignoreSnapshot {
				cfg.SnapshotDir = filepath.Join(dir, "snapshots-ignored")
			}
			w, err := wal.Open(walDir, 1<<20)
			if err != nil {
				t.Fatalf("reopen wal: %v", err)
			}
			start := time.Now()
			l, err := engine.NewLedger(cfg, w)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("reopen ledger: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if elapsed < best {
				best = elapsed
			}
		}
		return best
	}

	snapTime := measure(false)
	fullTime := measure(true)
	if snapTime > fullTime {
		t.Logf("note: snapshot path (%v) was not faster than full replay (%v) at this history size", snapTime, fullTime)
	}
	if got := l1.CurrentLSN(); got != headLSN || got == 0 {
		t.Fatalf("workload watermark %d unexpected vs head %d", got, headLSN)
	}
	if snapLSN == 0 {
		t.Fatal("snapshot LSN not recorded")
	}
	t.Logf("recovery of %d transfers (head LSN %d): snapshot-served=%v, full-replay=%v (snapshot cutoff LSN %d)",
		batches*perBatch, headLSN, snapTime, fullTime, snapLSN)
}
