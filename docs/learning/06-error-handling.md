# 06 — Database-Grade Error Handling

**Files in focus:** `internal/core/errors.go`, `internal/api/rest.go`,
`internal/api/server.go`, `internal/core/account.go`, `internal/engine/accounts.go`

Error handling in a ledger is not style — it is the product. A transfer that
"mostly succeeded" is a headline. This document covers Go's error mechanics
as this project uses them: typed errors, the `errors.Is`/`As` split, panic
policy, and mapping internal failures to API responses consistently.

---

## 1. Typed error structs, not sentinels

```go
// internal/core/errors.go
type ErrInsufficientFunds struct {
    AccountID [16]byte
    Balance   Uint128
    Amount    Uint128
}

func (e ErrInsufficientFunds) Error() string {
    return fmt.Sprintf("insufficient funds: account=%x balance=%s amount=%s",
        e.AccountID, String(e.Balance), String(e.Amount))
}
```

A sentinel (`var ErrInsufficient = errors.New(...)`) tells the caller *that*
an error occurred. A typed struct tells it *what* — the account, the balance
it had, the amount it wanted. Callers match on type and can read fields:

```go
var insufficient core.ErrInsufficientFunds
if errors.As(err, &insufficient) {
    // insufficient.AccountID, insufficient.Balance available
}
```

The distinction between the two matching functions, which Go developers
routinely mix up:

- `errors.Is(err, target)` — **identity** comparison, walking the wrap chain.
  Use for sentinel/empty-struct errors: `errors.Is(err, core.ErrNotLeader{})`
  works because `ErrNotLeader` is a comparable empty struct and `Is` falls
  back to `==` when the target is comparable.
- `errors.As(err, &target)` — **type extraction** into a pointer to the type,
  walking the chain. Use when you need the fields, as above.

Both traverse `Unwrap()` chains, so wrapping with `fmt.Errorf("...: %w", err)`
preserves matchability. If a function returns a *concrete* typed error
(not wrapped), a plain type assertion also works — but `errors.As` keeps
working when someone later wraps it, so prefer it.

## 2. Mapping internal errors to API failures — the C0.11 lesson

Two API surfaces (REST, gRPC) must agree on what each internal error *means*.
The REST gateway originally missed one:

```go
// internal/api/rest.go — current, post-C0.11
res, err := s.ledger.CreateTransfer(r.Context(), tr)
if err != nil {
    if errors.Is(err, core.ErrNotLeader{}) {
        writeError(w, http.StatusServiceUnavailable, err.Error())  // was: generic 500
        return
    }
    if errors.Is(err, core.ErrZeroAmount{}) || errors.Is(err, core.ErrSelfTransfer{}) {
        writeError(w, http.StatusBadRequest, err.Error())
        return
    }
    ...
}
```

The gRPC handler already mapped `ErrNotLeader` to `codes.Unavailable`; REST
fell through to 500, telling clients the *server* was broken when the truth
was "you hit a read-only follower — retry the primary." 500 vs 503 is not
cosmetics: load balancers and retry policies treat them differently. The
mapping tables as they now stand:

| Internal error | REST | gRPC |
|---|---|---|
| `ErrNotLeader{}` | 503 | `Unavailable` |
| `ErrZeroAmount`, `ErrSelfTransfer` | 400 | `InvalidArgument` |
| `ErrAccountNotFound` | 404 | `NotFound` |
| `ErrInsufficientFunds` | 422 | `FailedPrecondition` |
| `ErrDuplicateAccountID` | 409 | `AlreadyExists` |
| anything else | 500 | `Internal` |

The engineering discipline: when a new error type is born, both mapping
tables are edited in the same change, and the proof tests
(`TestRESTFollowerReturnsServiceUnavailableOnWrites`) pin the behavior.

## 3. Validation at the boundary — the C0.12 lesson

```go
func parseHexID(s string) ([16]byte, error) {
    var id [16]byte
    s = strings.TrimPrefix(s, "0x")
    b, err := hex.DecodeString(s)
    if err != nil { return id, err }
    if len(b) != 16 {
        return id, errors.New("id must be exactly 16 bytes (32 hex chars)")
    }
    copy(id[:], b)
    return id, nil
}
```

The original accepted shorter input and left-padded it into a `[16]byte` —
so `"01"` and `"00000000000000000000000000000001"` were silently the same
account. Lenient parsing at a trust boundary creates *aliases*: identities
the caller never chose. Rules this codebase now follows:

- Parse exactly; reject anything else with an error that says what *is*
  accepted ("exactly 16 bytes (32 hex chars)").
- Distinguish "bad syntax" (400) from "well-formed but unknown" (404). The
  old code couldn't, which is why the test asserts 400 on `GET /v1/accounts/01`.

## 4. Panic policy: where panics are allowed to live

```go
// internal/core/account.go — still present, C0.7 on the roadmap
func Balance(a Account) Uint128 {
    bal, err := Sub(a.PostedCredits, a.PostedDebits)
    if err != nil {
        panic("account invariant violated: posted debits exceed posted credits")
    }
    return bal
}
```

The Go norm — panic for programmer bugs, error for operational failure —
needs sharpening for a database. The project's policy:

1. **Library/package APIs return errors**, always. `ApplyDebit` returning
   `ErrBalanceOverflow` (post-C0.7a) is correct; it *used* to panic, which
   made a recoverable state issue fatal to whatever process imported it.
2. **An unrecoverable invariant violation may panic — deliberately** — but
   only after recording that it happened. If debits ever exceed credits, the
   database's reason to exist is void; continuing silently is worse than a
   loud death. The planned C0.7 upgrade makes this explicit: increment an
   `invariant_violations` metric, log, then crash — so the *alert* fires.
3. **Server boundaries recover panics into errors** (gRPC middleware), so one
   bad request cannot kill the process.
4. **Construction-time panics are fine** (`NewRingBuffer` on a bad size) —
   but the roadmap moves even those to errors so `cmd/` fails fast with a
   clean message. `panic` at `main` scope and `panic` inside the writer loop
   are different blast radii: a panic in the single-writer goroutine takes
   the whole server down mid-batch.

`recover()` appears in exactly one place by design (the gRPC recovery
interceptor). Pervasive `recover` is how bugs hide.

## 5. Error values in the hot path

`ValidateBatch` returns `[]error` — one outcome per event, positional:

```go
// internal/engine/transfers.go
outcomes := make([]error, len(events))
for i := range events {
    outcomes[i] = accounts.ValidateTransfer(events[i].Transfer)
}
```

Positional outcomes (not a `map[event]error`) preserve ordering and allow
`nil` for success without allocation — `nil` interface is a zero-value
interface header, free. The loop later acks each caller with its own
outcome: batch processing, per-item results — the TigerBeetle shape, which
the future batched API (D1.1) formalizes.

One cost worth knowing: `ValidateTransfer` and `ApplyBatch`'s re-check
duplicate logic (documented in the roadmap's C0.10 notes). When validation
drifts from application, you get "validated OK, failed on apply" — an
error class that should be impossible. Where correctness matters twice,
extract one function both paths call.

> **Exercise 1.** Wrap `ErrAccountNotFound` with `fmt.Errorf("lookup: %w", ...)`
> in one call site and confirm both `errors.As` and the REST 404 mapping
> still work. Then change the wrap to `%v` and see `errors.As` break — that
> is why the codebase standardizes on `%w`.
>
> **Exercise 2.** The mapping table in §2 is missing `ErrAccountFrozen` /
> `ErrAccountClosed` (both returned by `ValidateTransfer`). Map them (403 /
> `FailedPrecondition` are defensible) and add proof tests. Notice how the
> *absence* of a test is how C0.11 survived.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Typed errors with fields | `core/errors.go` | Sentinels say *that*; structs say *what* |
| `Is` vs `As` | API handlers | `Is` for identity; `As` to extract fields |
| `%w` wrapping | all call sites | Wrap to preserve matchability |
| Consistent mapping | REST + gRPC tables | New error ⇒ both tables, same commit, plus tests |
| Strict parsing | `parseHexID` | Reject, never coerce, at trust boundaries |
| Panic policy | `Balance`, loop | Errors in libraries; loud deliberate death for broken invariants; recover only at server edges |
| Positional outcomes | `ValidateBatch` | Batch process, per-item results, `nil` is free |
