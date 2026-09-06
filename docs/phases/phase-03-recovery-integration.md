# Phase 3 — Recovery Integration & Double-Entry Invariant Deep-Dive

## 1. Objective & Problem Statement

When the ledger server restarts (whether gracefully or following an ungraceful crash / kill command), the engine MUST guarantee that:
1. All durably committed WAL transfers are fully replayed in exact chronological sequence (LSN order).
2. The fundamental double-entry invariant holds true across all accounts:
$$\sum \text{PostedDebits} = \sum \text{PostedCredits}$$
3. No partial or corrupt transfer record alters the account balances.

Phase 3 wires startup recovery directly into `cmd/server/main.go` and introduces comprehensive crash recovery integration tests (`tests/integration/crash_recovery_test.go`).

---

## 2. Core Go Language Concepts & Engineering Internals

### A. Replay Handler Pattern
`Ledger.Recover()` registers a closure handler with `wal.WAL.Recover()`:

```go
func (l *Ledger) Recover() error {
    return l.wal.Recover(func(r wal.Record) error {
        if r.Type != wal.RecordTypeTransfer {
            return nil
        }
        t, err := decodeTransferPayload(r.Payload)
        if err != nil {
            return err
        }
        events := []TransferEvent{{Transfer: t}}
        outcomes := []error{nil}
        ApplyBatch(events, outcomes, l.accounts)
        return outcomes[0]
    })
}
```

* **Go Concept (Higher-Order Functions & Closures)**: Passing an inline function `func(Record) error` allows `wal.Recover` to handle disk iteration and decoding while `Ledger` retains total control over domain state mutations (`ApplyBatch`).

---

### B. Invariant Validation Gate
After replaying all valid log segments, the integration test executes `TestAccountManagerBalanceInvariant`:

```go
func VerifyDoubleEntryInvariant(accs []core.Account) bool {
    var totalDebits core.Uint128
    var totalCredits core.Uint128

    for _, a := range accs {
        var err error
        totalDebits, err = core.Add(totalDebits, a.PostedDebits)
        if err != nil {
            return false
        }
        totalCredits, err = core.Add(totalCredits, a.PostedCredits)
        if err != nil {
            return false
        }
    }
    return core.Cmp(totalDebits, totalCredits) == 0
}
```

---

### C. Simulated Crash & Truncated Tail Replay
`tests/integration/crash_recovery_test.go` verifies crash resiliency by:
1. Seeding initial accounts and processing 1,000 active transfers.
2. Abruptly closing the WAL segment without executing graceful server shutdown.
3. Appending truncated garbage bytes to simulate an incomplete disk block write.
4. Re-opening the ledger engine and verifying that post-recovery balances match expected mathematical ground truth.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **Recovery Ordering** | Sequential LSN Replay | Parallel Replay across Workers | Replaying in parallel introduces race conditions and out-of-order execution bugs. Replaying sequentially maintains strict deterministic state reconstruction. |
| **Verification Gate** | Explicit Invariant Assertions | Hope-based startup | Running explicit `SUM(debits) == SUM(credits)` checks before listening for API traffic prevents serving requests on corrupted state. |
