# 🧪 Aequitas Ledger — Complete Verification & Testing Playbook

> **Audience**: Principal Engineers, Systems Operators, Compliance Officers, and Security Auditors.  
> **Guarantees**: Zero-panic execution, lock-free deterministic state transitions, 4KB-aligned direct I/O, strict double-entry balance conservation, and offline verifiable WAL integrity.

---

## 📑 Table of Contents

1. [Quick-Start Verification (One-Command)](#1-quick-start-verification-one-command)
2. [Automated Test Suite Matrix](#2-automated-test-suite-matrix)
3. [Zero-Panic Policy Gate (`make panic-gate`)](#3-zero-panic-policy-gate-make-panic-gate)
4. [Financial Domain Depth Tests](#4-financial-domain-depth-tests)
   - [Two-Phase Holds (`FlagPending`, `FlagPostPending`, `FlagVoidPending`)](#two-phase-holds)
   - [Atomic Linked Chains (`FlagLinked`)](#atomic-linked-chains)
   - [Multi-Tenant Partitioning (`Ledger uint32`)](#multi-tenant-partitioning)
5. [Storage & Direct I/O Verification](#5-storage--direct-io-verification)
6. [High Availability & Replication Verification (mTLS)](#6-high-availability--replication-verification-mtls)
7. [Independent Offline WAL Auditor (`ledger-cli audit`)](#7-independent-offline-wal-auditor-ledger-cli-audit)
8. [Interactive REST & gRPC Verification (Manual Walkthrough)](#8-interactive-rest--grpc-verification-manual-walkthrough)
9. [Chaos & Fault Injection Testing](#9-chaos--fault-injection-testing)

---

## 1. Quick-Start Verification (One-Command)

The repository provides a turnkey, colorized 7-step verification suite that builds all binaries, validates the zero-panic policy, starts a sandboxed daemon, provisions multi-tenant accounts, exercises two-phase holds and linked transfer chains, and terminates by running an independent offline auditor on the generated WAL.

```bash
# Option A: Run directly with make
make demo

# Option B: Run via Python 3
python3 scripts/run-demo.py
```

### Expected Output
```text
==============================================================================
  ⚡ AEQUITAS LEDGER — SYSTEM VERIFICATION HARNESS ⚡
==============================================================================
  ℹ️   Architecture Model              : Single-Writer Event Loop · Group-Commit WAL · Lock-Free MPMC
  ℹ️   Storage Engine                  : O_DIRECT 4KB-Aligned Buffer · 64MB Preallocated Segments
  ℹ️   Financial Invariants            : Double-Entry Balance Conservation (SUM(Credits) - SUM(Debits) == 0)
  ℹ️   Execution Guarantees            : Zero-Panic Policy · 0 Data Races · Byte-Exact Struct Layouts

============================================================================
  🔒 1. PANIC GATE & CODEBASE INTEGRITY VERIFICATION
============================================================================
  ✅  [VERIFIED]  Zero-Panic Policy Gate passed successfully (0 panics in internal/)

============================================================================
  🧪 2. UNIT & CODEC STRUCTURE SIZE ASSERTER
============================================================================
  ✅  [VERIFIED]  Unit & Codec tests passed (sizeof(Account)=128B, sizeof(Transfer)=144B, 0 allocs)

============================================================================
  🚀 3. SINGLE-WRITER ENGINE COMPILATION & DAEMON BOOT
============================================================================
  ✅  [VERIFIED]  Compiled server and ledger-cli binaries cleanly
  ✅  [VERIFIED]  Process Liveness Check (/healthz): {'status': 'UP'}
  ✅  [VERIFIED]  Process Readiness Check (/readyz): {'status': 'READY'}

============================================================================
  🏢 4. MULTI-TENANT ACCOUNT PROVISIONING & SCHEMA VALIDATION
============================================================================
  ✅  [VERIFIED]  Provisioned Tenant Alpha Primary Account #0001
  ℹ️   Ledger Partition ID             : 100
  ℹ️   Initial Posted Credits          : 10000
  ℹ️   Chart of Accounts Code          : 1000
  ✅  [VERIFIED]  Provisioned Tenant Alpha Secondary Account #0002
  ✅  [VERIFIED]  Provisioned Tenant Beta Account #0003 in Ledger 200
  ✅  [VERIFIED]  Cross-Tenant transfer attempt successfully rejected (ErrLedgerMismatch)

============================================================================
  ⚙️ 5. TWO-PHASE HOLDS (RESERVE, SETTLE, VOID, TIMEOUT)
============================================================================
  ✅  [VERIFIED]  Reserved 2,000 USD hold on Account #0001 (FlagPending)
  ℹ️   Account #0001 Posted Balance    : 10000
  ℹ️   Account #0001 Available Balance : 8000
  ℹ️   Account #0001 Pending Debits    : 2000
  ✅  [VERIFIED]  Settled 1,500 USD partial hold; released 500 USD excess reserve
  ℹ️   Post-Settlement Posted Balance  : 8500
  ℹ️   Post-Settlement Available Balance: 8000

============================================================================
  🔗 6. LINKED TRANSFER CHAINS (ATOMIC MULTI-LEG ESCROW)
============================================================================
  ✅  [VERIFIED]  Linked chain atomic rollback verified! Leg 1 rolled back cleanly when Leg 2 failed.

============================================================================
  🔍 7. INDEPENDENT WAL AUDITOR & CONSERVATION VERIFICATION
============================================================================
  ✅  [VERIFIED]  Independent WAL Auditor executed cleanly!
WAL audit: .../data_demo/wal
  segments:   1
  records:    10
  batches:    5
  accounts:   3
  transfers:  2
  result:     PASS — no violations
```

---

## 2. Automated Test Suite Matrix

Execute the complete testing suite across all packages:

```bash
# Run all unit, integration, and audit tests with the demo harness
make test-all

# Run the test suite with Go's race detector active
go test -race ./...
```

| Test Target | Command | Scope |
| :--- | :--- | :--- |
| **Unit Tests** | `go test -v ./tests/unit/...` | Wire codecs, memory alignment, `Uint128` math overflow protection, lock-free ring buffers. |
| **Integration Suite** | `go test -v ./tests/integration/...` | Full engine, recovery from segment corruptions, two-phase holds, linked transfers, snapshot compaction. |
| **Independent Auditor** | `go test -v ./internal/audit/...` | CRC32 bit flips, torn tail detection, out-of-order LSNs, multi-tenant balance conservation. |
| **Direct I/O & Storage** | `go test -v ./internal/wal/...` | 4KB alignment validation, file segment preallocation (`posix_fallocate`), group-commit batch sync. |
| **Replication Suite** | `go test -v ./internal/replication/...` | Streaming WAL log replication, follower state catchup, mTLS handshake, adversarial network partition. |

---

## 3. Zero-Panic Policy Gate (`make panic-gate`)

Financial systems must never crash unexpectedly or leave corrupted un-synced disk state. Under Aequitas architecture, **no code inside `internal/` is permitted to invoke `panic()`**. Every failure mode returns a typed domain error.

Run the panic gate:
```bash
make panic-gate
```

If any developer accidentally introduces `panic()`, CI and local builds will immediately reject the code with line-numbered offending locations.

---

## 4. Financial Domain Depth Tests

### Two-Phase Holds
Validates card authorizations, escrow reservations, holds, partial capture, and release.

Run integration tests:
```bash
go test -v ./tests/integration/ -run TestTwoPhase
```

**Lifecycle Verified**:
1. **Hold Reservation (`FlagPending = 0x01`)**: Reserves balance from `available_balance` without debiting `posted_debits`.
2. **Partial Post (`FlagPostPending = 0x02`)**: Captures authorized amount, decrements pending hold, posts final debits/credits, and releases the excess back to available funds.
3. **Full Void (`FlagVoidPending = 0x04`)**: Reverts all pending reservations.
4. **Logical Clock Timeout**: Automatic expiry of uncaptured holds without external cron jobs.

### Atomic Linked Chains
Validates multi-leg atomic escrow transfers (e.g., Buyer $\to$ Escrow $\to$ Seller $\to$ Fee Account). If *any* leg fails validation, the entire chain rolls back immediately with zero state mutation and zero WAL writes.

```bash
go test -v ./tests/integration/ -run TestLinked
```

### Multi-Tenant Partitioning
Validates multi-tenant isolation where each ledger partition strictly enforces $\sum \text{Credits} - \sum \text{Debits} = 0$. Cross-ledger transfers without an explicit intermediary bridge are rejected with `ErrLedgerMismatch`.

```bash
go test -v ./tests/integration/ -run TestLedgerMismatch
```

---

## 5. Storage & Direct I/O Verification

Aequitas uses `O_DIRECT` (on Linux) with 4KB sector alignment and 64 MiB segment preallocation to avoid OS page cache lock contention and jitter.

Run the direct I/O test suite:
```bash
go test -v ./internal/wal/ -run TestDirectIO
```

Key guarantees verified:
- Buffer addresses and lengths are strictly 4096-byte aligned.
- File sizes grow in fixed 64 MiB increments (`F_PREALLOCATE` / `fallocate`), eliminating file metadata allocation locks on the write path.
- Non-direct fallback runs cleanly in dev/macOS environments while honoring zero page cache thrashing.

---

## 6. High Availability & Replication Verification (mTLS)

Run the replication test suite to verify primary-follower log streaming:

```bash
go test -v ./tests/integration/ -run TestReplication
```

Test scenarios covered:
- Primary streaming committed LSN frames over TLS/mTLS to follower nodes.
- Follower replaying WAL frames directly into its read-only shadow engine.
- Follower read-consistency: follower rejects write attempts with `ErrNotLeader`.
- Adversarial tests: simulated network delay, disconnect during batch replication, and automatic reconnection catching up to primary LSN.

---

## 7. Independent Offline WAL Auditor (`ledger-cli audit`)

The independent auditor (`internal/audit`) shares **zero code** with the execution engine. It re-implements the wire frame decoder, CRC32 verifier, LSN sequence monitor, and double-entry conservation equations from scratch.

Run the auditor against any live or archived WAL directory:

```bash
# Build the CLI tool
go build -o bin/ledger-cli ./cmd/ledger-cli

# Run the audit pass
./bin/ledger-cli audit -wal ./data/wal
```

### What the Auditor Validates:
1. **CRC32 Frame Integrity**: Every frame payload checksum matches byte-for-byte.
2. **Batch Atomicity**: Records per batch match the batch commit marker count exactly.
3. **LSN Monotonicity**: LSNs are strictly sequential ($LSN_n > LSN_{n-1}$).
4. **Conservation of Money**: For every tenant ledger $L$:
   $$\sum_{a \in \text{Accounts}_L} \text{Credits}_a - \sum_{a \in \text{Accounts}_L} \text{Debits}_a = \text{InjectedCredits}_L$$
5. **Non-Negative Balances**: No account balance drops below zero ($Debits \le Credits$).
6. **Zero Duplicate Identifiers**: No transfer ID or idempotency key is reused across distinct operations.

---

## 8. Interactive REST & gRPC Verification (Manual Walkthrough)

### 1. Boot the Server
```bash
# In Terminal 1:
export WAL_DIR="./data/wal"
export SNAPSHOT_DIR="./data/snapshots"
export REST_PORT="8080"
export PORT="50051"

go run ./cmd/server
```

### 2. Check Health & Readiness
```bash
# Liveness probe
curl -s http://localhost:8080/healthz

# Readiness probe (returns READY once recovery completes)
curl -s http://localhost:8080/readyz
```

### 3. Create Multi-Tenant Accounts
```bash
# Create Account 1 in Ledger 100 with $1,000.00
curl -X POST http://localhost:8080/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "id": "00000000000000000000000000000001",
    "currency": "USD",
    "initial_credits": "100000",
    "ledger": 100,
    "code": 1000
  }'

# Create Account 2 in Ledger 100 with $0.00
curl -X POST http://localhost:8080/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "id": "00000000000000000000000000000002",
    "currency": "USD",
    "ledger": 100,
    "code": 1000
  }'
```

### 4. Execute a Two-Phase Hold
```bash
# Reserve $250.00 hold (Flags: 1 = Pending)
curl -X POST http://localhost:8080/v1/transfers \
  -H "Content-Type: application/json" \
  -d '{
    "id": "00000000000000000000000000000050",
    "debit_account_id": "00000000000000000000000000000001",
    "credit_account_id": "00000000000000000000000000000002",
    "amount": "25000",
    "flags": 1,
    "ledger": 100
  }'

# Inspect balances: available balance is reduced, posted balance remains unchanged
curl -s http://localhost:8080/v1/accounts/00000000000000000000000000000001
```

### 5. Settle the Hold
```bash
# Capture $200.00 of the $250.00 hold (Flags: 2 = PostPending, $50.00 released)
curl -X POST http://localhost:8080/v1/transfers \
  -H "Content-Type: application/json" \
  -d '{
    "id": "00000000000000000000000000000050",
    "amount": "20000",
    "flags": 2,
    "ledger": 100
  }'
```

---

## 9. Chaos & Fault Injection Testing

To guarantee crash resilience against power loss or hardware failure:

1. **Unannounced Process Termination (`SIGKILL`)**:
   ```bash
   kill -9 $(pgrep server)
   ```
2. **Corrupted Segment Bit Flips**:
   The engine's recovery scanner automatically detects torn tails at segment boundaries and recovers clean state up to the last valid LSN batch.
3. **Auditor Confirmation**:
   Running `ledger-cli audit -wal ./data/wal` verifies whether the crash caused any invariant violations or torn records.

---
*For further architectural deep-dives and math specifications, see [docs/architecture-guide.md](architecture-guide.md).*
