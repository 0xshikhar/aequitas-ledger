# ADR 004 — Deterministic batch ordering (no map iteration in the state machine)

**Status:** Accepted  
**Bucket:** 1 — Core write mechanics (non-deferrable trap)  
**Affects:** `internal/engine/loop.go`, `internal/engine/transfers.go`, `internal/wal/recovery.go`

---

## Context

The defining correctness property of this ledger is **deterministic replay**: replaying the WAL must always produce exactly the same account balances, regardless of how many times it is replayed, on any machine, at any time.

This property is what makes crash recovery trustworthy. If replay is non-deterministic, you cannot rely on the recovered state being correct — which means you cannot rely on the ledger at all.

### The trap: Go's randomised map iteration

Go intentionally randomises `map` iteration order on every program run (introduced in Go 1.0 and made more aggressive in Go 1.3). This is a deliberate language design choice to prevent developers from accidentally depending on map iteration order.

Consider this seemingly reasonable optimisation inside the event loop:

```go
// DANGEROUS — do not implement this
func (e *Engine) applyBatch(events []TransferEvent) {
    // Group by debit account to "optimise" balance lookups
    byAccount := make(map[core.AccountID][]TransferEvent)
    for _, ev := range events {
        byAccount[ev.Transfer.DebitAccountID] = append(byAccount[...], ev)
    }

    // BUG: map iteration order is random across process restarts
    for accountID, group := range byAccount {
        for _, ev := range group {
            applyTransfer(ev.Transfer)
        }
    }
}
```

If Transfer A (debit account X, $100) and Transfer B (debit account X, $50) both arrive in the same batch, this code will apply them in random order across different process restarts. If the account balance is exactly $100:

- Run 1: A then B → A succeeds, B fails (insufficient funds). Final balance: $0.
- Run 2: B then A → B succeeds, A succeeds. Final balance: $0 — wait, same result here, but...
- Run 3: A then B again — but what if the batch on the PREVIOUS crash also had other dependent transfers? The cascading effect of ordering changes corrupts the global state.

The WAL records the raw transfers in submission order. If the state machine applies them in a different order than the WAL recorded them, recovery produces a different result than live execution. **The ledger is now inconsistent.**

This is not a theoretical concern. It is the most common correctness bug in batch-processing financial systems written by engineers who understand concurrency but underestimate Go's map randomisation.

---

## Decision

**Rule: The single-writer event loop must process every batch as a flat slice, in the exact order records were pulled from the ring buffer. No map-based grouping, sorting, or reordering is permitted in the state machine's execution path.**

```go
// internal/engine/loop.go — correct implementation

func (l *EventLoop) processBatch(events []TransferEvent) {
    // Step 1: Validate all transfers in order — pure reads, no mutations
    outcomes := make([]error, len(events))
    for i, ev := range events {
        outcomes[i] = l.accounts.ValidateTransfer(ev.Transfer)
    }

    // Step 2: WAL append — records written in slice order == ring buffer order
    records := makeRecords(events)        // deterministic: same slice order
    l.wal.AppendBatch(records)
    l.wal.Sync()

    // Step 3: Apply mutations in slice order — deterministic
    for i, ev := range events {
        if outcomes[i] == nil {
            l.accounts.ApplyTransfer(ev.Transfer) // mutates balance directly
        }
    }

    // Step 4: Ack in slice order
    for i, ev := range events {
        ev.Result <- outcomes[i]
    }
}
```

### The validation-before-mutation split

Note that validation (Step 1) happens before any mutation (Step 3). This is intentional:

- **Validation is pure:** It reads account balances but does not change them. All validations in a batch see the balances as they were at the start of the batch.
- **Mutation is sequential:** Changes are applied one by one, in order. Each transfer sees the balance as modified by all previous transfers in the same batch.

This means if Transfer A (debit account X, $100) and Transfer B (debit account X, $50) are in the same batch with a starting balance of $100:

- Validation of A: balance $100 ≥ $100 → valid
- Validation of B: balance $100 ≥ $50 → valid (snapshot at batch start)
- Apply A: balance $100 → $0
- Apply B: balance $0 < $50 → fail (ErrInsufficientFunds applied at mutation time)

This two-phase approach (validate-all then apply-in-order) is more lenient than strictly sequential processing (where B would also be validated against the post-A balance of $0). It is the correct behaviour for a batch ledger: each transfer's validity is assessed against the balance at batch arrival time, not against previous transfers in the same batch. This is consistent with how Postgres handles concurrent transactions at REPEATABLE READ isolation.

If strict sequential semantics are required (A's deduction visible to B's validation within the same batch), use smaller batch sizes or route to separate batches.

---

## WAL record ordering

The WAL `AppendBatch` function must write records in the exact order they appear in the input slice:

```go
// internal/wal/wal.go
func (w *WAL) AppendBatch(records []Record) (startLSN int64, err error) {
    // Records are written in records[0], records[1], ... records[n-1] order.
    // This order must match the order in which the state machine applies them.
    // Do NOT sort, group, or reorder records inside this function.
    for _, rec := range records {
        if err := w.writeRecord(rec); err != nil {
            return 0, err
        }
    }
    return w.currentLSN, nil
}
```

Recovery in `recovery.go` reads and replays records in the same sequential order. Because the WAL is an append-only log, this is automatic — file offset order equals write order equals replay order.

---

## What you ARE allowed to optimise (without violating ordering)

These optimisations are safe because they do not change the sequence of state mutations:

- **Pre-fetch account pointers before the mutation loop.** Build a `map[AccountID]*Account` index before Step 3 to avoid repeated map lookups inside the mutation loop. This is a read-only operation and does not affect ordering.
- **Pre-allocate the `outcomes` slice.** Reuse it across batches with `sync.Pool` to avoid per-batch allocation.
- **Parallel WAL serialisation.** The `makeRecords` function (converting `TransferEvent` to `Record` bytes) can be parallelised if it becomes a bottleneck, as long as the final byte buffer is written to the segment file in original slice order.

These are safe. Sorting, grouping, or reordering mutations is not.

---

## Testing requirements

`tests/unit/batch_test.go` must include:

**Ordering preservation test:**
Submit 1,000 transfers to a single account in order T1…T1000, each spending the balance left by the previous. Verify that after processing, the final balance equals the result of applying T1…T1000 in that exact sequence — not any permutation.

**Crash-recovery determinism test** (`tests/integration/crash_recovery_test.go`):
1. Run 10 batches of 1,000 transfers each against a live engine
2. Record final account balances (ground truth)
3. Kill the process mid-run
4. Restart and replay WAL
5. Assert recovered balances == ground truth from step 2

**Map-freedom lint check:**
Add a `go vet` or `staticcheck` custom rule (or a comment-enforced convention) that flags any use of `range` over a `map` inside `internal/engine/`. Maps are allowed in the engine for lookups but never for iteration in the execution path.

---

## Alternatives considered

**Sort batch by account ID before processing (rejected).** Deterministic (same sort key → same order) but changes the semantic meaning of the ledger — transfers are no longer applied in submission order, which breaks audit trails and makes it impossible to reason about which of two concurrent transfers "won."

**Use a sorted map / treemap (rejected).** Deterministic but still changes submission order. Same semantic problem as above.

**Single transfer per batch (rejected).** Trivially deterministic but eliminates group commit and destroys throughput. The entire purpose of batching is to amortise fsync cost across many transfers.
