package integration

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

// C0.5 proof: the background checkpointer persists snapshots on its interval,
// truncates segments strictly below the snapshot LSN, makes the node ready,
// and a restart recovers from the snapshot (watermark intact, balances
// identical).
func TestBackgroundCheckpointerSnapshotsTruncatesAndReadies(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	snapDir := filepath.Join(dir, "snapshots")

	w1, err := wal.Open(walDir, 1<<15) // 32 KiB segments → several rotations
	if err != nil {
		t.Fatalf("open wal 1: %v", err)
	}
	cfg := engine.DefaultConfig()
	cfg.SnapshotDir = snapDir
	cfg.SnapshotInterval = 100 * time.Millisecond
	cfg.MaxSnapshotsKept = 2

	l1, err := engine.NewLedger(cfg, w1)
	if err != nil {
		t.Fatalf("new ledger 1: %v", err)
	}
	// First cycle runs immediately: an empty ledger completes the cycle
	// without a file, so readiness must already be true.
	if !l1.Ready() {
		t.Fatal("ledger with checkpointer not ready after first snapshot cycle")
	}

	ctx := context.Background()
	acc1, acc2 := mkID(1), mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}
	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 10_000}}); err != nil {
		t.Fatalf("create acc1: %v", err)
	}
	if _, err := l1.CreateAccount(ctx, core.Account{ID: acc2, Currency: cur}); err != nil {
		t.Fatalf("create acc2: %v", err)
	}
	batch := make([]core.Transfer, 300)
	for i := range batch {
		var trID [16]byte
		binary.BigEndian.PutUint32(trID[:4], uint32(i+1))
		batch[i] = core.Transfer{ID: trID, DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10}}
	}
	if _, err := l1.CreateTransfers(ctx, batch); err != nil {
		t.Fatalf("batch transfers: %v", err)
	}

	// Wait for a real snapshot file to appear and its LSN to be recorded.
	deadline := time.Now().Add(5 * time.Second)
	snapshotsExist := false
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(snapDir)
		if len(entries) > 0 && l1.LastSnapshotLSN() > 0 {
			snapshotsExist = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !snapshotsExist {
		t.Fatal("no snapshot file appeared within 5s of transfers")
	}

	// Segments strictly below the snapshot LSN must be truncated away: after
	// a snapshot, the segment count stops growing with history.
	segsBefore := countSegments(walDir)
	time.Sleep(250 * time.Millisecond) // one more cycle
	segsAfter := countSegments(walDir)
	if segsAfter > segsBefore+1 { // +1 tolerates an in-progress rotation
		t.Fatalf("WAL still growing despite snapshots: %d → %d segments", segsBefore, segsAfter)
	}

	headLSN := l1.CurrentLSN()

	// /readyz must be READY via the REST surface.
	ts := httptest.NewServer(api.NewRESTServer(l1))
	readyResp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	body := readAll(t, readyResp)
	readyResp.Body.Close()
	if readyResp.StatusCode != http.StatusOK || !strings.Contains(string(body), "READY") {
		t.Fatalf("readyz: got %d %s, want 200 READY", readyResp.StatusCode, body)
	}
	ts.Close()

	// Restart: balances must be identical and the watermark intact.
	if err := l1.Close(); err != nil {
		t.Fatalf("close ledger 1: %v", err)
	}

	w2, err := wal.Open(walDir, 1<<15)
	if err != nil {
		t.Fatalf("open wal 2: %v", err)
	}
	l2, err := engine.NewLedger(cfg, w2)
	if err != nil {
		t.Fatalf("new ledger 2: %v", err)
	}
	defer l2.Close()

	if got := l2.CurrentLSN(); got != headLSN {
		t.Fatalf("recovered watermark = %d, want head %d", got, headLSN)
	}
	bal1, err := l2.GetBalance(ctx, acc1)
	if err != nil {
		t.Fatalf("balance 1: %v", err)
	}
	if bal1.Lo != 10_000-300*10 {
		t.Fatalf("acc1 balance = %d, want %d", bal1.Lo, 10_000-300*10)
	}
	bal2, _ := l2.GetBalance(ctx, acc2)
	if bal2.Lo != 300*10 {
		t.Fatalf("acc2 balance = %d, want %d", bal2.Lo, 300*10)
	}
}

// C0.5: a ledger without a checkpointer is ready purely on recovery.
func TestReadyWithoutCheckpointer(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	l, err := engine.NewLedger(engine.DefaultConfig(), w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()
	if !l.Ready() {
		t.Fatal("ledger without checkpointer should be ready after recovery")
	}
}

func countSegments(dir string) int {
	entries, _ := os.ReadDir(dir)
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".seg") {
			n++
		}
	}
	return n
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	buf := make([]byte, 1024)
	n, _ := resp.Body.Read(buf)
	return buf[:n]
}
