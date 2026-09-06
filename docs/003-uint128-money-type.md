# ADR 003 — Uint128 as the canonical money type

**Status:** Accepted  
**Bucket:** 2 — Operational realities (non-deferrable)  
**Affects:** `internal/core/uint128.go`, all domain types, WAL record encoding, proto definitions

---

## Context

This is the one item from Bucket 2 that cannot be deferred. Every other operational concern (checkpointer, read path, eviction) can be bolted on after the engine works. The money type cannot. It dictates:

- The binary layout of every WAL record ever written
- The protobuf wire format for every API message
- The arithmetic semantics of the entire state machine

Changing it after the fact requires migrating the WAL format, rewriting all protos, and auditing every arithmetic operation. That is a full rewrite.

### Why int64 is insufficient

`int64` can represent values up to approximately `9.2 × 10¹⁸`. This feels large until you consider:

- **Native crypto tokens:** ETH uses 18 decimal places. 1 ETH = `1_000_000_000_000_000_000` wei. The maximum `int64` value is approximately 9.2 ETH worth of wei. Any wallet balance over 9 ETH overflows.
- **Sei network:** Uses 18 decimal precision. The same overflow applies.
- **Hedera HBAR:** 8 decimals, but total supply is 50 billion HBAR — `5 × 10¹⁸` tinybars — which already approaches `int64` ceiling and provides no headroom for multi-asset aggregation.
- **Stablecoin ledgers at scale:** A ledger tracking total USDC supply (currently ~40 billion USD at 6 decimal places = `4 × 10¹⁶`) is safe in `int64`, but aggregated multi-asset positions or internal clearing amounts can overflow under pathological conditions.

Even if the immediate use case fits in `int64`, building a ledger that claims Web3 compatibility while silently overflowing is a critical financial bug that has caused real losses in production systems.

### Why not `big.Int`

`big.Int` handles arbitrary precision but is heap-allocated. Every arithmetic operation allocates. At 200k TPS, that is 200,000 GC-visible allocations per second just from balance arithmetic — before considering request structs, WAL records, or anything else. The GC pause profile becomes the throughput ceiling, which directly contradicts the goal of the single-writer event loop.

---

## Decision

Define a custom `Uint128` struct composed of two `uint64` fields (no pointers, stack-allocated):

```go
// internal/core/uint128.go

type Uint128 struct {
    Lo uint64  // least significant 64 bits
    Hi uint64  // most significant 64 bits
}
```

This is a **value type** — zero heap allocation, GC-invisible (the GC does not scan `uint64` fields). All arithmetic is implemented as inline operations on `Lo` and `Hi`.

### Core functions in `uint128.go`

```
Add(a, b Uint128) (Uint128, error)         // returns ErrOverflow if Hi overflows
Sub(a, b Uint128) (Uint128, error)         // returns ErrUnderflow if result would be negative
Mul64(a Uint128, b uint64) (Uint128, error) // multiply by a scalar — used for fee calculation
Cmp(a, b Uint128) int                      // -1, 0, 1 — used for balance sufficiency check
IsZero(a Uint128) bool
FromUint64(v uint64) Uint128               // convenience constructor
FromString(s string) (Uint128, error)      // decimal string → Uint128
String(a Uint128) string                   // Uint128 → decimal string (for logging/display)
MarshalBinary(a Uint128) []byte            // 16 bytes, big-endian — for WAL encoding
UnmarshalBinary(b []byte) (Uint128, error) // inverse
```

### WAL record encoding

`Uint128` serialises to exactly 16 bytes in WAL records:

```
[ Hi: 8 bytes big-endian ][ Lo: 8 bytes big-endian ]
```

This is fixed-width, enabling O(1) record length calculation without variable-length encoding.

### Protobuf representation

Go's protobuf has no native `uint128` type. Two options:

**Option A — two uint64 fields (chosen):**
```protobuf
message Money {
  uint64 amount_lo = 1;
  uint64 amount_hi = 2;
}
```
Strongly typed, no parsing required on the receiving end, zero allocation.

**Option B — bytes field:** Encode as 16-byte big-endian `bytes`. Requires parsing on every use. Rejected for core money fields.

Use Option A for `Account.balance`, `Transfer.amount`, and any other money field in `ledger.proto`.

---

## Overflow detection strategy

Every arithmetic operation in the state machine must be explicitly checked. The single-writer loop is not allowed to panic — a panicking event loop brings down the entire server and drops all in-flight requests in the batch.

```go
// In transfers.go — called by the single-writer loop
func validateAndApply(acct *core.Account, t core.Transfer) error {
    newBalance, err := core.Sub(acct.Balance, t.Amount)
    if err != nil {
        return core.ErrInsufficientFunds{
            AccountID: acct.ID,
            Balance:   acct.Balance,
            Amount:    t.Amount,
        }
    }
    acct.Balance = newBalance
    return nil
}
```

Overflow on the credit side (balance too large to represent) is also checked:

```go
newBalance, err := core.Add(acct.Balance, t.Amount)
if err != nil {
    return core.ErrBalanceOverflow{AccountID: acct.ID}
}
```

---

## Use in Account and Transfer types

```go
// internal/core/account.go
type Account struct {
    ID             [16]byte   // UUID as fixed-size array — no pointer, no string allocation
    Currency       [4]byte    // ISO 4217 or ticker — fixed-size
    PostedDebits   Uint128
    PostedCredits  Uint128
    Flags          uint32     // frozen, closed, padding-reserved
    _              [12]byte   // padding to 64-byte cache line alignment
}

// internal/core/transfer.go
type Transfer struct {
    ID             [16]byte
    DebitAccountID [16]byte
    CreditAccountID [16]byte
    Amount         Uint128
    Timestamp      int64      // Unix nanoseconds
    Flags          uint32
    _              [4]byte    // padding
}
```

Both `Account` and `Transfer` are designed to fit exactly or evenly into 64-byte cache lines. A slice of `Account` values is cache-friendly; a slice of `*Account` pointers is not — each pointer dereference is a potential cache miss under sequential scan.

---

## Testing requirements

`tests/unit/uint128_test.go` must cover:

- Addition without overflow
- Addition at the boundary (max `uint64` carry into `Hi`)
- Addition overflow (`Hi` would overflow) → `ErrOverflow`
- Subtraction without underflow
- Subtraction to zero
- Subtraction underflow → `ErrUnderflow`
- Round-trip `MarshalBinary` / `UnmarshalBinary`
- `FromString` / `String` round-trip for values > `int64` max
- **Fuzz test:** `go test -fuzz=FuzzUint128Add` — property: `Add(a, b)` where no overflow ↔ `a.Lo + b.Lo` as `uint64` with carry semantics matches the result

---

## Alternatives considered

**`int64` (rejected).** Overflows for native crypto tokens at production balances. Foundation of entire WAL format — cannot be changed after first write.

**`uint64` (rejected).** Unsigned removes negative balance bugs but still overflows for 18-decimal tokens.

**`big.Int` (rejected).** Heap-allocated. GC pressure at 200k TPS makes it incompatible with the single-writer performance target.

**`decimal` library (`shopspring/decimal`) (rejected).** Heap-allocated, designed for human-readable decimal arithmetic. Not suitable for a binary WAL or high-throughput state machine.

**`float64` (hard rejected).** Floating-point arithmetic is non-associative and non-deterministic across platforms. A ledger using `float64` will accumulate rounding errors and produce different WAL replay results on different hardware. Never use floating-point for money.
