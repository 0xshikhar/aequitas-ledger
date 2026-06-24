package bench

import (
	"context"
	"crypto/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"aequitas-ledger/internal/core"
)

type TransferExecutor interface {
	CreateTransfer(ctx context.Context, t core.Transfer) (core.Transfer, error)
}

type Result struct {
	TotalOps   int64
	Duration   time.Duration
	TPS        float64
	P50Latency time.Duration
	P95Latency time.Duration
	P99Latency time.Duration
	ErrorCount int64
}

type Harness struct {
	accounts []core.Account
}

func NewHarness(accounts []core.Account) *Harness {
	return &Harness{accounts: accounts}
}

func (h *Harness) Run(ctx context.Context, exec TransferExecutor, concurrency int, duration time.Duration) Result {
	var wg sync.WaitGroup
	var ops int64
	var errs int64

	latencies := make([][]time.Duration, concurrency)
	stop := make(chan struct{})

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		workerID := i
		latencies[workerID] = make([]time.Duration, 0, 10000)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localOps int64
			var localErrs int64
			localLat := make([]time.Duration, 0, 10000)

			for {
				select {
				case <-stop:
					atomic.AddInt64(&ops, localOps)
					atomic.AddInt64(&errs, localErrs)
					latencies[workerID] = localLat
					return
				default:
					tr := h.generateRandomTransfer(workerID)
					t0 := time.Now()
					_, err := exec.CreateTransfer(ctx, tr)
					elapsed := time.Since(t0)
					if err != nil {
						localErrs++
					} else {
						localOps++
						localLat = append(localLat, elapsed)
					}
				}
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	totalTime := time.Since(start)

	// Aggregate latencies
	var allLat []time.Duration
	for _, workerLat := range latencies {
		allLat = append(allLat, workerLat...)
	}

	sort.Slice(allLat, func(i, j int) bool { return allLat[i] < allLat[j] })

	var p50, p95, p99 time.Duration
	n := len(allLat)
	if n > 0 {
		p50 = allLat[n*50/100]
		p95 = allLat[n*95/100]
		p99 = allLat[n*99/100]
	}

	tps := 0.0
	if totalTime.Seconds() > 0 {
		tps = float64(ops) / totalTime.Seconds()
	}

	return Result{
		TotalOps:   ops,
		Duration:   totalTime,
		TPS:        tps,
		P50Latency: p50,
		P95Latency: p95,
		P99Latency: p99,
		ErrorCount: errs,
	}
}

func (h *Harness) generateRandomTransfer(workerID int) core.Transfer {
	numAccs := len(h.accounts)
	if numAccs < 2 {
		panic("harness requires at least 2 accounts")
	}

	srcIdx := workerID % numAccs
	dstIdx := (workerID + 1) % numAccs

	var id [16]byte
	_, _ = rand.Read(id[:])

	var key [32]byte
	_, _ = rand.Read(key[:])

	return core.Transfer{
		ID:              id,
		DebitAccountID:  h.accounts[srcIdx].ID,
		CreditAccountID: h.accounts[dstIdx].ID,
		Amount:          core.FromUint64(100),
		IdempotencyKey:  key,
	}
}

func GenerateAccounts(n int, initialBalance uint64) []core.Account {
	accs := make([]core.Account, n)
	var curr [4]byte
	copy(curr[:], "USD")

	for i := 0; i < n; i++ {
		var id [16]byte
		id[15] = byte(i + 1)
		id[14] = byte((i + 1) >> 8)
		accs[i] = core.Account{
			ID:            id,
			Currency:      curr,
			PostedCredits: core.FromUint64(initialBalance),
		}
	}
	return accs
}
