# Engine layer — complete function map

The engine layer (`internal/engine/`) is the heart of the ledger. It owns all mutable state. No other package is allowed to mutate account balances. This document specifies every significant function across every file.

---

## internal/core/ — domain types (no dependencies, no logic)

### uint128.go
```
Add(a, b Uint128) (Uint128, error)
    Adds two Uint128 values. Returns ErrOverflow if Hi would overflow uint64.
    Uses: (Lo sum, carry) = bits.Add64(a.Lo, b.Lo, 0); Hi sum with carry check.

Sub(a, b Uint128) (Uint128, error)
    Subtracts b from a. Returns ErrUnderflow if a < b.
    Check: if a.Hi < b.Hi || (a.Hi == b.Hi && a.Lo < b.Lo) → underflow.

Cmp(a, b Uint128) int
    Returns -1, 0, or 1. Compares Hi first, then Lo.

IsZero(a Uint128) bool
    Returns a.Hi == 0 && a.Lo == 0.

FromUint64(v uint64) Uint128
    Returns Uint128{Lo: v, Hi: 0}.

FromString(s string) (Uint128, error)
    Parses a decimal string. Iterates digits, multiplying accumulator by 10.
    Returns ErrOverflow if value exceeds 2^128 - 1.

String(a Uint128) string
    Converts to decimal string. Divides by 10^19 repeatedly.

MarshalBinary(a Uint128) []byte
    Returns 16 bytes: Hi as 8 bytes big-endian, Lo as 8 bytes big-endian.

UnmarshalBinary(b []byte) (Uint128, error)
    Inverse of MarshalBinary. Returns error if len(b) != 16.
```

### account.go
```
type Account struct {
    ID              [16]byte   // UUID bytes — no string allocation
    Currency        [4]byte    // e.g. "USDC", "ETH\x00"
    PostedDebits    Uint128    // sum of all completed debit transfers
    PostedCredits   Uint128    // sum of all completed credit transfers
    Flags           uint32     // bit flags: frozen=1, closed=2
    _               [12]byte   // padding to 64-byte cache line
}

Balance(a Account) Uint128
    Returns PostedCredits - PostedDebits.
    Panics if PostedDebits > PostedCredits (invariant violation — should never happen).

IsFrozen(a Account) bool
IsClosed(a Account) bool
```

### transfer.go
```
type Transfer struct {
    ID              [16]byte
    DebitAccountID  [16]byte
    CreditAccountID [16]byte
    Amount          Uint128
    IdempotencyKey  [32]byte   // SHA-256 of client-provided key string
    Timestamp       int64      // Unix nanoseconds — set by the loop, not the client
    Flags           uint32
    _               [4]byte    // padding
}
```

### errors.go
```
ErrInsufficientFunds{AccountID, Balance, Amount}
ErrAccountNotFound{AccountID}
ErrAccountFrozen{AccountID}
ErrAccountClosed{AccountID}
ErrSelfTransfer{AccountID}
ErrZeroAmount{}
ErrCurrencyMismatch{DebitCurrency, CreditCurrency}
ErrBalanceOverflow{AccountID}
ErrIdempotencyConflict{Key}    // key in-flight from another goroutine
ErrBufferFull{}                // ring buffer at capacity — client should retry
ErrDuplicateTransferID{ID}
```

---

## internal/engine/event.go

```
type TransferEvent struct {
    Transfer core.Transfer
    Result   chan error   // buffered(1) — loop sends once, handler reads once
}

NewTransferEvent(t core.Transfer) TransferEvent
    Returns TransferEvent{Transfer: t, Result: make(chan error, 1)}

type AccountCreateEvent struct {
    Account core.Account
    Result  chan error
}
```

---

## internal/engine/ring_buffer.go

```
type RingBuffer struct {
    buf  []TransferEvent   // fixed-size, pre-allocated at construction
    head uint64            // atomic — write position (producers advance this)
    tail uint64            // owned by single consumer — no atomic needed
    mask uint64            // = cap - 1, for fast modulo
}

NewRingBuffer(size int) *RingBuffer
    size must be a power of 2. Pre-allocates buf slice.
    Recommended default: 1 << 20 = 1,048,576 slots.

Submit(ev TransferEvent) error
    Called by gRPC handlers (concurrent producers).
    Uses CAS on head to claim a slot.
    Returns ErrBufferFull if (head - tail) >= cap.
    Lock-free: no mutex.

DrainBatch(max int) []TransferEvent
    Called exclusively by the single-writer loop (sole consumer).
    Returns up to max events from tail to min(tail+max, head).
    Advances tail by the number of events drained.
    Returns nil if no events available.
    Not thread-safe — only the loop may call this.

Len() int
    Returns approximate number of pending events. May be stale.
    Used only for metrics/monitoring.
```

---

## internal/engine/batch.go

```
type Batcher struct {
    rb           *RingBuffer
    maxBatchSize int           // default: 10_000
    timeout      time.Duration // default: 1ms
}

NewBatcher(rb *RingBuffer, maxSize int, timeout time.Duration) *Batcher

Collect() []TransferEvent
    Blocks until either:
      - rb.Len() >= maxBatchSize, OR
      - timeout has elapsed since the first event arrived in the current batch
    Returns all available events, up to maxBatchSize.
    If rb is empty, blocks in a tight spin for up to timeout, then returns nil.

    Implementation note: avoid time.Sleep in the spin loop — it yields the OS
    scheduler and adds ~1ms of latency. Use runtime.Gosched() for cooperative
    yielding, or a brief spin with atomic reads of rb.Len().
```

---

## internal/engine/accounts.go

**No mutexes. No locks. All functions called only from the single-writer loop.**

```
type AccountManager struct {
    accounts []core.Account           // flat slice, index by position
    index    map[[16]byte]int          // account ID → slice index
}

NewAccountManager(initialCapacity int) *AccountManager

Create(a core.Account) error
    Appends account to the accounts slice.
    Adds ID → index mapping.
    Returns ErrDuplicateAccountID if ID already exists.

Get(id [16]byte) (*core.Account, error)
    Returns pointer into accounts slice (not a copy).
    Returns ErrAccountNotFound if not found.
    Pointer is valid until the next Create call (which may grow the slice).

ValidateTransfer(t core.Transfer) error
    Pure read — does not mutate any state.
    Checks: debit account exists, credit account exists, not same account,
            amount > 0, currencies match, debit account not frozen/closed,
            credit account not frozen/closed, debit balance >= amount.
    Returns the first error encountered, nil if all checks pass.

ApplyDebit(id [16]byte, amount core.Uint128)
    Adds amount to account.PostedDebits.
    Panics if resulting PostedDebits > PostedCredits + amount (invariant break).
    Must only be called after ValidateTransfer returned nil for this transfer.

ApplyCredit(id [16]byte, amount core.Uint128)
    Adds amount to account.PostedCredits.
    Returns error on Uint128 overflow (ErrBalanceOverflow).

ApplyBatch(events []TransferEvent, outcomes []error)
    Applies debits and credits for all events where outcomes[i] == nil.
    Processes in slice order — no reordering.
    Calls ApplyDebit then ApplyCredit for each valid transfer.

Snapshot() []core.Account
    Returns a copy of the accounts slice for the checkpointer.
    Called between batches by the snapshot coordination mechanism.

Len() int
    Returns number of accounts. Used for metrics.
```

---

## internal/engine/transfers.go

**All functions called only from the single-writer loop.**

```
ValidateBatch(events []TransferEvent, accounts *AccountManager) []error
    Validates all transfers in batch order.
    Returns outcomes slice of length len(events).
    Each element is nil (valid) or an error.
    Does NOT mutate any account state.
    Validates against account state at the START of the batch
    (snapshot semantics — see ADR 004).

ApplyBatch(events []TransferEvent, outcomes []error, accounts *AccountManager)
    For each event where outcomes[i] == nil:
        accounts.ApplyDebit(event.Transfer.DebitAccountID, event.Transfer.Amount)
        accounts.ApplyCredit(event.Transfer.CreditAccountID, event.Transfer.Amount)
    Processes in slice order.
    Re-checks balance sufficiency at mutation time (balance may have changed
    from earlier transfers in the same batch).
    Sets outcomes[i] = ErrInsufficientFunds if balance check fails at mutation.
```

---

## internal/engine/idempotency.go

```
type IdempotencyStore struct {
    maxSize int
    ttl     time.Duration
    mu      sync.RWMutex
    entries *lruMap        // doubly-linked list + typed map
    size    int
}

NewIdempotencyStore(maxSize int, ttl time.Duration) *IdempotencyStore

CheckAndReserve(key [32]byte) (existing core.Transfer, found bool, err error)
    Acquires RLock first for fast path (key found → return cached).
    Upgrades to Lock only on miss to insert a pending sentinel.
    Returns:
        (transfer, true, nil)   — cached result, do not re-execute
        (zero, false, nil)      — reserved, caller must Commit or Rollback
        (zero, false, ErrIdempotencyConflict) — another goroutine has reserved

Commit(key [32]byte, result core.Transfer)
    Acquires Lock.
    Replaces pending sentinel with committed result.
    Updates LRU order.

Rollback(key [32]byte)
    Acquires Lock.
    Removes pending sentinel (allows next attempt to re-execute).

evictExpired()
    Background goroutine calls this periodically.
    Acquires Lock, scans from LRU tail, removes entries past TTL.

evictLRU()
    Called when size >= maxSize, inside Commit (already holding Lock).
    Removes the least-recently-used committed entry.
```

---

## internal/engine/ledger.go

```
type Ledger struct {
    loop     *EventLoop
    rb       *RingBuffer
    idempKey *IdempotencyStore
    accounts *AccountManager   // read-only reference for GetBalance, GetAccount
    metrics  *observability.Metrics
}

NewLedger(cfg config.Config, wal *wal.WAL) (*Ledger, error)
    Constructs all sub-components.
    Calls wal.Recover() to rebuild account state before starting the loop.
    Starts the EventLoop goroutine.
    Starts the Checkpointer goroutine.

CreateAccount(ctx context.Context, a core.Account) error
    Enqueues AccountCreateEvent to the ring buffer.
    Blocks on Result channel.
    Returns error or nil.

CreateTransfer(ctx context.Context, t core.Transfer) (core.Transfer, error)
    1. Derive idempotency key: SHA-256 of t.IdempotencyKey header
    2. CheckAndReserve — if hit, return cached transfer
    3. Enqueue TransferEvent to ring buffer — return ErrBufferFull if full
    4. Block on Result channel
    5. On success: Commit idempotency key
    6. On failure: Rollback idempotency key
    7. Return result

GetAccount(ctx context.Context, id [16]byte) (core.Account, error)
    Reads directly from AccountManager — no ring buffer, no blocking.
    Safe because AccountManager.Get returns a value copy for external callers.
    NOTE: balance reflects state at the time of the call, not a point-in-time snapshot.

GetBalance(ctx context.Context, id [16]byte) (core.Uint128, error)
    Convenience wrapper over GetAccount.

Recover() error
    Called at startup before the event loop starts.
    Replays WAL records, calling accounts.Create or accounts.ApplyBatch.
    Loads the latest snapshot first if available.
```

---

## internal/engine/loop.go

```
type EventLoop struct {
    batcher         *Batcher
    accounts        *AccountManager
    transfers       // functions from transfers.go
    wal             *wal.WAL
    snapshotReqs    chan snapshotRequest   // buffered(1)
    metrics         *observability.Metrics
    currentLSN      int64
}

NewEventLoop(b *Batcher, a *AccountManager, w *wal.WAL, m *observability.Metrics) *EventLoop

Run(ctx context.Context)
    The single goroutine. Main loop:
        for {
            handleSnapshotRequest()  // non-blocking check
            batch := batcher.Collect()
            if len(batch) == 0 { continue }
            processBatch(batch)
        }
    Exits when ctx is cancelled (graceful shutdown).

processBatch(events []TransferEvent)
    outcomes := transfers.ValidateBatch(events, accounts)
    records  := makeWALRecords(events, outcomes)
    wal.AppendBatch(records)
    wal.Sync()                            // ONE fsync
    currentLSN = wal.CurrentLSN()
    transfers.ApplyBatch(events, outcomes, accounts)
    for i, ev := range events {
        ev.Result <- outcomes[i]          // unblock gRPC handlers
    }
    metrics.RecordBatch(len(events), syncDuration)

handleSnapshotRequest()
    select {
    case req := <-snapshotReqs:
        copy := accounts.Snapshot()
        req.result <- snapshotView{accounts: copy, lsn: currentLSN}
    default:
    }

RequestSnapshotView() ([]core.Account, int64)
    Called by the checkpointer goroutine.
    Sends to snapshotReqs channel, blocks on result.

makeWALRecords(events []TransferEvent, outcomes []error) []wal.Record
    For each event where outcome == nil: encode as RecordTypeTransfer.
    For each event where outcome != nil: encode as RecordTypeTransferRejected
        (so rejected transfers are also in the audit log with their error code).
    Returns records in same order as events.
```
