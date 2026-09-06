# 01 — Value Semantics & Memory Layout

**Files in focus:** `internal/core/uint128.go`, `internal/core/account.go`,
`internal/core/transfer.go`, `internal/core/codec.go`,
`internal/engine/ring_buffer.go`

Go is a **pass-by-value language**: every assignment, argument passing, and
return copies the value. This project leans into that fact harder than most
Go codebases, because a financial database wants predictable memory. This
document walks through what "value" really means and how the domain types
exploit it.

---

## 1. A 128-bit integer as two words

```go
// internal/core/uint128.go
type Uint128 struct {
    Lo uint64
    Hi uint64
}
```

Why not `math/big.Int`? Because `big.Int` is a heap-allocated, pointer-backed,
mutable type with internal slice reallocation on every operation. In a hot
path processing hundreds of thousands of transfers per second, that is GC
pressure you cannot afford. `Uint128` is 16 bytes, fits in two registers, and
copies by value — `acc = mul` in `FromString` copies 16 bytes and touches no
allocator.

The arithmetic uses the `math/bits` intrinsics, which compile to single
hardware instructions for add-with-carry:

```go
func Add(a, b Uint128) (Uint128, error) {
    lo, carry := bits.Add64(a.Lo, b.Lo, 0)
    hi, carryHi := bits.Add64(a.Hi, b.Hi, carry)
    if carryHi != 0 {
        return Uint128{}, ErrUint128Overflow
    }
    return Uint128{Lo: lo, Hi: hi}, nil
}
```

`bits.Add64` returns the sum *and* the carry-out, exactly like the ADC
instruction. The carry from the low word feeds the high word's add. If the
high add carries out, the true result needed 129 bits — overflow, returned as
an error, never as a silently wrapped value. **Financial rule: arithmetic
never lies quietly.**

Note the API shape: `Add(a, b) (Uint128, error)` — free functions, not
methods. Methods would suggest `a.Add(b)` mutates `a`; these are pure value
operations. The one place `math/big` *is* used is `String()`, a display-only
cold path — the right tool in the right layer.

> **Exercise 1.** Implement `Mul` for `Uint128` using `bits.Mul64`. Study
> `mul64` first: it multiplies a 128-bit by a 64-bit and shows the partial
> product pattern. What is the overflow condition for full 128×128?

## 2. Arrays are values: `[16]byte` identifiers

```go
type Transfer struct {
    ID              [16]byte
    DebitAccountID  [16]byte
    CreditAccountID [16]byte
    Amount          Uint128
    IdempotencyKey  [32]byte
    Timestamp       int64
    Flags           uint32
}
```

Every identifier is a **fixed-size array**, not a slice, not a string. Arrays
in Go are values: comparing `t.DebitAccountID == t.CreditAccountID` compares
all 16 bytes inline with no allocation, and `[16]byte` works as a map key
(`map[[16]byte]int` in `AccountManager`) because it is comparable. A `string`
would also work as a key but hides whether it holds raw bytes or hex — and a
`[]byte` would be unusable as a key (slices are not comparable) and would
point at mutable, shared memory.

The cost is copy pressure: every `Transfer` copy moves 16+16+16+16+32 = 96
bytes of arrays plus the rest. That is deliberate — 112-byte value structs
copy in a handful of instructions, while pointer-chasing through heap objects
costs cache misses and GC scanning.

## 3. Struct size, padding, and the compile-time guard

```go
// internal/core/account.go
type Account struct {
    ID            [16]byte
    Currency      [4]byte
    PostedDebits  Uint128
    PostedCredits Uint128
    Flags         uint32
    _             [4]byte
}

// Compile-time guard to keep Account cache-friendly and predictable.
var _ [64 - unsafe.Sizeof(Account{})]byte
```

Two tricks worth memorizing:

**Explicit padding.** The Go compiler aligns struct fields to their natural
alignment: `uint64` fields start at offsets divisible by 8. `Currency
[4]byte` ends at offset 20, so `PostedDebits` would be padded to offset 24
anyway — the `_ [4]byte` makes the padding *explicit and intentional*, and
makes the wire format in `EncodeAccountPayload` (exactly 64 bytes) match the
in-memory layout conceptually.

**The size guard.** `unsafe.Sizeof(Account{})` is a compile-time constant. If
someone adds a field and pushes the struct past 64 bytes, `64 - Sizeof`
becomes negative, and an array type with a negative length is a **compile
error**. This is a build-time assertion with zero runtime cost — the Go
idiom for "this invariant must never drift." Snapshots serialize accounts as
64-byte records (`snapshot.AccountSize = 64`), so the guard is protecting a
*file format*, not just aesthetics.

> **Exercise 2.** Add a `Name string` field to `Account` and recompile. Read
> the error. Then think: why would a `string` field be wrong here beyond the
> size? (Hint: what does a snapshot file do 5 years from now when it
> dereferences a string that pointed into a heap that no longer exists?)

## 4. Slices are headers: the 1.2 MB batch

A slice is not its data — it is a three-word header:

```go
type slice struct {
    ptr unsafe.Pointer
    len int
    cap int
}
```

`append` grows the backing array when `len == cap`, typically doubling. This
is the root of the most expensive bug this project fixed (C0.15):

```go
// internal/engine/ring_buffer.go — BEFORE (the bug)
out := make([]TransferEvent, 0, max)   // max = MaxBatchSize = 10_000

// AFTER (the fix)
pending := head - rb.tail
if uint64(max) > pending {
    max = int(pending)
}
out := make([]TransferEvent, 0, max)
```

`TransferEvent` contains a `Transfer` (~112 bytes) plus a channel (8-byte
pointer) and padding — roughly 120 bytes. 10,000 × 120 = **1.2 MB allocated
per batch, even for a batch of one transfer**, because `DrainBatch` sized its
result slice by the *maximum* batch size instead of the *pending* count. The
benchmark before/after in `tests/unit/engine_alloc_bench_test.go`:
`CreateTransferAllocs` went from 1,204,884 B/op to 656 B/op — a 1,800×
reduction from one line.

The lessons:

- `make([]T, 0, cap)` allocates `cap × sizeof(T)` **up front**, regardless of
  how many elements you ever add.
- Preallocation is only a win when the bound is also a good *estimate*.
  Bounding by `maxBatchSize` (a limit) vs `pending` (an observation) is the
  difference between 1.2 MB and 120 bytes.
- You find this with `b.ReportAllocs()` + `-benchmem`, never by reading code.
  Document 08 shows the full hunt.

## 5. The zero value is a real value

Every Go type has a zero value: `0` for numbers, `""` for strings, `nil` for
pointers/slices/maps/channels, and **all fields zeroed** for structs. The zero
value is not "uninitialized" — it is a perfectly valid member of the type, and
your code will meet it whether you planned for it or not.

This project shipped a real bug because of that (C0.18). The idempotency key
is `[32]byte`; "no idempotency key" is naturally represented as the zero
value `[32]byte{}`. But `CreateTransfer` registered and deduplicated the zero
key like any other:

```go
// BEFORE: the zero key behaved as a normal key
key := t.IdempotencyKey
existing, found, err := l.idempotKey.CheckAndReserve(key) // zero key reserved!
```

Two keyless transfers — different IDs, different accounts — and the second
silently returned the *first one's result*. The fix made the zero value's
meaning explicit:

```go
// AFTER: zero key means "no deduplication"
if t.IdempotencyKey != [32]byte{} {
    // ... reserve/commit/rollback ...
}
```

The discipline: **for every field of every struct you design, decide what the
zero value means, and write that decision down or enforce it in code.** The
recovery path had already assumed "zero key = no key" (`if t.IdempotencyKey
!= [32]byte{}` in `Ledger.RecoverFromSnapshot`); the live path disagreed.
When two code paths disagree about a zero value, you have a latent bug.

Same family, different flavor (C0.12): `parseHexID` used to left-pad short
hex strings — `"01"` and `"0000...01"` became the same `[16]byte`, silently.
The fix rejects anything that is not exactly 16 bytes. Zero-adjacent values
(zero-padded, zero-length, empty) deserve explicit, strict handling at every
boundary.

## 6. Maps: fast, unordered, and not for the state machine

`AccountManager` uses both structures, each for what it's good at:

```go
type AccountManager struct {
    accounts []core.Account        // iteration + dense storage
    index    map[[16]byte]int      // O(1) lookup by ID
}
```

Go maps have three properties that matter for a database core:

1. **Iteration order is randomized by design.** The runtime scrambles the
   start position deliberately, so nobody accidentally depends on it. That is
   why ADR-004 mandates flat slices in the *batch* path: `ValidateBatch` and
   `ApplyBatch` iterate `[]TransferEvent` in submission order, determinism the
   WAL replay depends on. Map iteration appears nowhere in the apply path.
2. **Maps hold pointers internally.** A map of structs scans pointer-heavy
   buckets during GC. The slice+index split keeps the hot data dense and
   lets GC skip it (it contains no pointers — 64-byte value structs).
3. **A map read during a write is a data race**, even for "just a lookup."
   Document 02 covers how the read path was routed around this.

> **Exercise 3.** Write a benchmark comparing `map[[16]byte]int` lookup vs
> binary search over a sorted `[][16]byte` for 1M entries, with
> `-benchmem`. Where does the map win, and where does locality win?

## 7. Strings vs []byte

`Currency [4]byte` — a fixed array, not a string. Strings in Go are a
pointer+length header pointing at immutable bytes; putting one in a
fixed-size record means a pointer that must never be serialized. `FromString`
in `uint128.go` iterates `s[i]` byte-by-byte — strings are indexable like
read-only byte slices, and `len(s)` is O(1). `strings.TrimPrefix(s, "0x")`
returns a substring sharing the original backing array (no copy) — cheap, but
it means a tiny substring can pin a huge string in memory. Converting
`string ↔ []byte` copies; in hot paths, avoid round-trips.

## Recap

| Concept | Where you saw it | Rule of thumb |
|---|---|---|
| Value semantics | `Uint128`, `Transfer` copies | Small fixed structs by value; pointers for sharing/mutation |
| Arrays vs slices | `[16]byte` IDs | Arrays when size is part of the type; slices when dynamic |
| Compile-time guards | `var _ [64 - unsafe.Sizeof(...)]byte` | Assert invariants at build time, cost nothing at runtime |
| Slice capacity | `DrainBatch` 1.2 MB bug | Preallocate to an *estimate*, never a *limit* |
| Zero values | C0.18 zero idempotency key | Decide and enforce what zero means for every field |
| Maps | `AccountManager.index` | Lookup yes; ordering/determinism never |
