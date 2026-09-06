# Phase 6 — Benchmark Baseline & Load Harness Deep-Dive

## 1. Objective & Problem Statement

To prove the high-throughput architecture claims of **Aequitas Ledger**, we must benchmark it directly against a standard enterprise relational database setup (PostgreSQL) executing identical double-entry transaction invariants.

Key objectives:
1. Implement a PostgreSQL baseline double-entry store using `pgxpool` (`internal/store/postgres/store.go`).
2. Solve SQL concurrency deadlocks using deterministic account lock ordering (`FOR UPDATE`).
3. Build a multi-worker benchmark harness calculating TPS, p50/p95/p99 latency percentiles, and error rates (`bench/harness.go`, `cmd/bench/main.go`).

---

## 2. Core Go Language Concepts & Engineering Internals

### A. PostgreSQL Deadlock Prevention via Deterministic Lock Ordering
In PostgreSQL, if Transaction A locks Account 1 then Account 2, while Transaction B locks Account 2 then Account 1 concurrently, PostgreSQL encounters a **deadlock cycle** and aborts one transaction with error `40P01`.

To guarantee zero deadlocks under maximum SQL concurrency:
```go
// Lock accounts in deterministic ID string order
id1 := fmt.Sprintf("%x", tr.DebitAccountID[:])
id2 := fmt.Sprintf("%x", tr.CreditAccountID[:])

first, second := id1, id2
if first > second {
    first, second = second, first
}

// Always lock 'first' before 'second'
tx.QueryRow(ctx, `SELECT posted_debits, posted_credits FROM accounts WHERE id = $1 FOR UPDATE`, first)
tx.QueryRow(ctx, `SELECT posted_debits, posted_credits FROM accounts WHERE id = $1 FOR UPDATE`, second)
```

---

### B. High-Concurrency Benchmark Harness & Worker Pools
The benchmark harness (`bench/harness.go`) spawns $N$ concurrent worker goroutines that send continuous transfers over a specified duration:

```go
func (h *Harness) Run(ctx context.Context, exec TransferExecutor, concurrency int, duration time.Duration) Result {
    var wg sync.WaitGroup
    latencies := make([][]time.Duration, concurrency)
    stop := make(chan struct{})

    start := time.Now()
    for i := 0; i < concurrency; i++ {
        workerID := i
        wg.Add(1)
        go func() {
            defer wg.Done()
            // Per-worker thread local latency recording (avoids global lock)
            localLat := make([]time.Duration, 0, 10000)
            for {
                select {
                case <-stop:
                    latencies[workerID] = localLat
                    return
                default:
                    tr := h.generateRandomTransfer(workerID)
                    t0 := time.Now()
                    _, err := exec.CreateTransfer(ctx, tr)
                    elapsed := time.Since(t0)
                    if err == nil {
                        localLat = append(localLat, elapsed)
                    }
                }
            }
        }()
    }

    time.Sleep(duration)
    close(stop)
    wg.Wait()
    // Sort combined latencies and extract p50, p95, p99 percentiles
}
```

* **Go Concept (Worker Local Storage)**: Each worker goroutine writes to its own slice `latencies[workerID]`. This avoids a shared mutex across all worker goroutines, ensuring zero harness overhead during load testing.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **SQL Lock Strategy** | Deterministic ID Order `FOR UPDATE` | Optimistic Concurrency Control | OCC retries excessively under high account contention; deterministic locking guarantees transaction completion without retries. |
| **Harness Latency** | Per-worker slice aggregation | Mutex-protected slice | Mutex-protected slices introduce lock contention inside the bench test runner itself, masking true server throughput. |
