package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/replication"
	"aequitas-ledger/internal/wal"
)

// C0.8.1 proof: the follower preserves group commit — one local fsync per
// primary batch, never one per record. A primary that commits 2 account
// batches + 3 transfer batches must produce a follower WAL with ~5 commit
// markers for 152 data records, not 152.
func TestFollowerPreservesGroupCommit(t *testing.T) {
	dir := t.TempDir()
	primaryWAL, err := wal.Open(filepath.Join(dir, "primary"), 1<<20)
	if err != nil {
		t.Fatalf("open primary wal: %v", err)
	}
	primary, err := engine.NewLedger(engine.DefaultConfig(), primaryWAL)
	if err != nil {
		t.Fatalf("new primary: %v", err)
	}
	defer primary.Close()

	replAddr := "127.0.0.1:17101"
	replServer := replication.NewServer(replAddr, primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start replication server: %v", err)
	}
	defer replServer.Stop()

	followerWAL, err := wal.Open(filepath.Join(dir, "follower"), 1<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	acc1, acc2 := mkID(1), mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}
	follower := replication.NewFollower(replAddr, followerWAL)
	follower.Start()
	defer follower.Stop()

	ctx := context.Background()
	// One account batch + three transfer batches, all on the primary.
	_, err = primary.CreateAccounts(ctx, []core.Account{
		{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 1_000_000}},
		{ID: acc2, Currency: cur},
	})
	if err != nil {
		t.Fatalf("create accounts: %v", err)
	}
	for b := 0; b < 3; b++ {
		batch := make([]core.Transfer, 50)
		for i := range batch {
			n := uint64(b*50 + i + 1)
			batch[i] = core.Transfer{
				ID: mkID(1000 + n), DebitAccountID: acc1, CreditAccountID: acc2,
				Amount: core.Uint128{Lo: 1},
			}
		}
		if _, err := primary.CreateTransfers(ctx, batch); err != nil {
			t.Fatalf("transfer batch %d: %v", b, err)
		}
	}

	// Wait for the follower to catch up to the primary's final head.
	head := primary.CurrentLSN()
	waitFor(t, 5*time.Second, func() bool { return follower.LastLSN() >= head && follower.Lag() == 0 })

	// Count commit markers vs data records in the follower's WAL.
	dataRecords, commitRecords := countWALRecordTypes(followerWAL)
	if dataRecords != 2+150 {
		t.Fatalf("follower WAL has %d data records, want 152", dataRecords)
	}
	if commitRecords > 10 {
		t.Fatalf("follower WAL has %d commit markers for %d data records — group commit is broken (per-record fsyncs)", commitRecords, dataRecords)
	}
	t.Logf("follower WAL: %d data records, %d commit markers", dataRecords, commitRecords)
}

// C0.8.3 proof: a follower refuses streams from a stale primary. After
// accepting term 2, a follower pointed at a term-1 primary must not apply
// anything (split-brain protection).
func TestFollowerFencesStalePrimary(t *testing.T) {
	dir := t.TempDir()
	primaryWAL, err := wal.Open(filepath.Join(dir, "primary"), 1<<20)
	if err != nil {
		t.Fatalf("open primary wal: %v", err)
	}
	primary, err := engine.NewLedger(engine.DefaultConfig(), primaryWAL)
	if err != nil {
		t.Fatalf("new primary: %v", err)
	}
	defer primary.Close()

	ctx := context.Background()
	acc1, acc2 := mkID(1), mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}
	if _, err := primary.CreateAccounts(ctx, []core.Account{
		{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 1_000}},
		{ID: acc2, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}
	if _, err := primary.CreateTransfers(ctx, []core.Transfer{
		{ID: mkID(1), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10}},
	}); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	replAddr := "127.0.0.1:17102"
	replServer := replication.NewServer(replAddr, primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	followerWAL, err := wal.Open(filepath.Join(dir, "follower"), 1<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	follower := replication.NewFollower(replAddr, followerWAL)
	follower.Start()
	waitFor(t, 5*time.Second, func() bool { return follower.LastLSN() > 0 })
	follower.Stop()
	replServer.Stop()

	// Promote-era term bump: the primary's term file moves to 2 and a fresh
	// server carries it. The follower (persisted lastTerm 2) accepts it.
	if _, err := replication.BumpTerm(primaryWAL); err != nil {
		t.Fatalf("bump term: %v", err)
	}
	replServer2 := replication.NewServer(replAddr, primaryWAL)
	if err := replServer2.Start(); err != nil {
		t.Fatalf("start server v2: %v", err)
	}
	defer replServer2.Stop()

	follower2 := replication.NewFollower(replAddr, followerWAL)
	follower2.Start()
	defer follower2.Stop()
	waitFor(t, 5*time.Second, func() bool { return follower2.LastLSN() >= primary.CurrentLSN() })
	follower2.Stop()

	// Now a stale primary (fresh WAL dir → term 1) at a different address.
	staleWAL, err := wal.Open(filepath.Join(dir, "stale"), 1<<20)
	if err != nil {
		t.Fatalf("open stale wal: %v", err)
	}
	staleAddr := "127.0.0.1:17103"
	staleServer := replication.NewServer(staleAddr, staleWAL)
	if err := staleServer.Start(); err != nil {
		t.Fatalf("start stale server: %v", err)
	}
	defer staleServer.Stop()

	// The stale server streams from an EMPTY WAL: a follower that accepted
	// it would reset to zero state. The fenced follower must ignore it.
	follower3 := replication.NewFollower(staleAddr, followerWAL)
	follower3.Start()
	defer follower3.Stop()

	time.Sleep(400 * time.Millisecond) // several reconnect attempts at term 1
	lsn := follower3.LastLSN()
	accs := follower3.Accounts()
	if lsn <= 0 || len(accs) == 0 {
		t.Fatalf("fenced follower lost state: lastLSN=%d accounts=%d", lsn, len(accs))
	}
	// State must be exactly what the term-2 primary produced.
	bal1, err := follower3.GetAccount(acc1)
	if err != nil {
		t.Fatalf("get account from fenced follower: %v", err)
	}
	if got := balanceOf(t, bal1).Lo; got != 990 {
		t.Fatalf("fenced follower balance = %d, want 990 (state must be untouched by stale stream)", got)
	}
}

// C0.8.2 + restart-safety proof: a restarted follower resumes from its local
// WAL (no duplicate history), and a circuit-breaker full re-sync rebuilds
// state from scratch.
func TestFollowerRestartRecoveryAndResync(t *testing.T) {
	dir := t.TempDir()
	primaryWAL, err := wal.Open(filepath.Join(dir, "primary"), 1<<20)
	if err != nil {
		t.Fatalf("open primary wal: %v", err)
	}
	primary, err := engine.NewLedger(engine.DefaultConfig(), primaryWAL)
	if err != nil {
		t.Fatalf("new primary: %v", err)
	}
	defer primary.Close()

	ctx := context.Background()
	acc1, acc2 := mkID(1), mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}
	if _, err := primary.CreateAccounts(ctx, []core.Account{
		{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 1_000}},
		{ID: acc2, Currency: cur},
	}); err != nil {
		t.Fatalf("create accounts: %v", err)
	}
	writeTransfers := func(first, count int) {
		t.Helper()
		batch := make([]core.Transfer, count)
		for i := range batch {
			n := uint64(first + i)
			batch[i] = core.Transfer{ID: mkID(n), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 1}}
		}
		if _, err := primary.CreateTransfers(ctx, batch); err != nil {
			t.Fatalf("transfers: %v", err)
		}
	}
	writeTransfers(1, 10)

	replAddr := "127.0.0.1:17104"
	replServer := replication.NewServer(replAddr, primaryWAL)
	if err := replServer.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer replServer.Stop()

	followerWAL, err := wal.Open(filepath.Join(dir, "follower"), 1<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	follower := replication.NewFollower(replAddr, followerWAL)
	follower.Start()
	waitFor(t, 5*time.Second, func() bool { return follower.LastLSN() >= primary.CurrentLSN() })
	follower.Stop()

	// More history while the follower is down.
	writeTransfers(11, 10)

	// Restart: local-WAL recovery positions the follower at its true applied
	// LSN and the delta is streamed — no duplication.
	follower2 := replication.NewFollower(replAddr, followerWAL)
	follower2.Start()
	defer follower2.Stop()
	waitFor(t, 5*time.Second, func() bool { return follower2.LastLSN() >= primary.CurrentLSN() })

	bal1, err := follower2.GetAccount(acc1)
	if err != nil {
		t.Fatalf("follower account 1: %v", err)
	}
	primaryBal1, _ := primary.GetBalance(ctx, acc1)
	if got := balanceOf(t, bal1).Lo; got != primaryBal1.Lo {
		t.Fatalf("follower balance %d != primary %d (double-apply on restart?)", got, primaryBal1.Lo)
	}
	dataRecords, _ := countWALRecordTypes(followerWAL)
	if dataRecords != 2+20 {
		t.Fatalf("follower WAL has %d data records, want 22 (duplicated history after restart)", dataRecords)
	}

	// Circuit breaker: point a new follower at a dead address with fast
	// tunables; after BreakerLimit failures it performs a full re-sync
	// (state + WAL wiped). When the real server later becomes reachable at
	// that address, the follower rebuilds everything from LSN 1.
	deadAddr := "127.0.0.1:17105"
	followerWAL2, err := wal.Open(filepath.Join(dir, "follower2"), 1<<20)
	if err != nil {
		t.Fatalf("open follower2 wal: %v", err)
	}
	// Seed prior state so the reset is observable. A valid account record:
	// an invalid one would make recoverLocalState refuse to start the
	// follower at all (deliberate fail-safe).
	accPayload := core.EncodeAccountPayload(core.Account{ID: mkID(9), Currency: [4]byte{'U', 'S', 'D', 0}})
	_, _ = followerWAL2.AppendBatch([]wal.Record{{Type: wal.RecordTypeAccount, Payload: accPayload}})
	_ = followerWAL2.Sync()

	breaker := replication.NewFollower(deadAddr, followerWAL2)
	breaker.BackoffBase = 5 * time.Millisecond
	breaker.BackoffMax = 10 * time.Millisecond
	breaker.BreakerLimit = 3
	breaker.Start()
	defer breaker.Stop()

	// Wait for the breaker to trip (3 fast failures) and the WAL to reset.
	waitFor(t, 5*time.Second, func() bool {
		data, commits := countWALRecordTypes(followerWAL2)
		return data == 0 && commits == 0
	})

	// Now bring a server up at the dead address: the follower reconnects and
	// rebuilds the full state from LSN 1.
	replServer2 := replication.NewServer(deadAddr, primaryWAL)
	if err := replServer2.Start(); err != nil {
		t.Fatalf("start recovery server: %v", err)
	}
	defer replServer2.Stop()
	waitFor(t, 10*time.Second, func() bool {
		acc, err := breaker.GetAccount(acc1)
		return err == nil && balanceOf(t, acc).Lo == primaryBal1.Lo
	})
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// countWALRecordTypes walks w via the StreamReader, which — unlike Recover —
// delivers BatchCommit records, so the follower's own group-commit markers
// are countable.
func countWALRecordTypes(w *wal.WAL) (dataRecords, commitRecords int) {
	sr := w.NewStreamReader(0)
	defer sr.Close()
	for {
		r, ok, err := sr.Next()
		if err != nil || !ok {
			break
		}
		switch r.Type {
		case wal.RecordTypeBatchCommit:
			commitRecords++
		case wal.RecordTypeHead:
			// stream-only; never in a WAL
		default:
			dataRecords++
		}
	}
	return dataRecords, commitRecords
}
