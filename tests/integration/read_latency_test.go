package integration

import (
	"context"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

// C0.16: reads must not starve under heavy write load. Before the fix, the
// event loop's select-with-default serviced at most one queued read per loop
// iteration while Batcher.Collect drained up to MaxBatchSize events at once —
// so with continuous concurrent submitters the ring buffer never emptied and
// read latency grew without bound (queue position × batch-cycle time). The
// batcher now yields to pending control work, so every queued read is
// serviced at least once per batch cycle.
//
// The latency budget is one batch cycle: the batch wait plus the WAL fsync,
// which a fair (non-preemptive) single-writer loop cannot interrupt. This is
// why the bound is 10 ms on p99 rather than sub-millisecond.
func TestReadLatencyBoundedUnderWriteLoad(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	cfg := engine.DefaultConfig()
	l, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	if _, err := l.CreateAccount(ctx, core.Account{ID: mkID(1), Currency: [4]byte{'U', 'S', 'D', 0}, PostedCredits: core.Uint128{Lo: 1 << 40}}); err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: mkID(2), Currency: [4]byte{'U', 'S', 'D', 0}}); err != nil {
		t.Fatalf("create account: %v", err)
	}

	// Continuous concurrent submitters: the ring buffer stays non-empty for
	// the whole window, which is exactly the regime in which the old loop
	// starved control work.
	const writers = 16
	const perWriter = 500

	var writerWG sync.WaitGroup
	errCh := make(chan error, writers)
	for p := 0; p < writers; p++ {
		p := p
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			for i := 0; i < perWriter; i++ {
				n := uint64(p*perWriter + i + 1)
				var trID [16]byte
				var key [32]byte
				trID[0] = byte(n)
				trID[1] = byte(n >> 8)
				key[0] = byte(n)
				key[1] = byte(n >> 8)
				if _, err := l.CreateTransfer(ctx, core.Transfer{
					ID:             trID,
					DebitAccountID: mkID(1), CreditAccountID: mkID(2),
					Amount:         core.Uint128{Lo: 1},
					IdempotencyKey: key,
				}); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	writersDone := make(chan struct{})
	go func() {
		writerWG.Wait()
		close(writersDone)
	}()

	var latencies []time.Duration
	deadline := time.After(30 * time.Second)
reading:
	for {
		select {
		case <-writersDone:
			break reading
		case <-deadline:
			t.Fatal("writers did not finish in time")
		default:
		}
		start := time.Now()
		if _, err := l.GetAccount(ctx, mkID(1)); err != nil {
			t.Fatalf("get account: %v", err)
		}
		latencies = append(latencies, time.Since(start))
	}

	select {
	case err := <-errCh:
		t.Fatalf("writer failed: %v", err)
	default:
	}

	n := len(latencies)
	if n < 500 {
		t.Fatalf("only %d reads completed; reader starved entirely", n)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[n/2]
	p99 := latencies[n*99/100]
	if p50 > 2*time.Millisecond {
		t.Fatalf("read p50 = %v over %d reads, want < 2ms (reads queued behind batches)", p50, n)
	}
	if p99 > 10*time.Millisecond {
		t.Fatalf("read p99 = %v over %d reads, want < 10ms (reads starved under write load)", p99, n)
	}
	t.Logf("reads=%d p50=%v p99=%v max=%v", n, p50, p99, latencies[n-1])
}
