# ADR-001: Single-Writer Event Loop Model

- **Status**: Accepted
- **Deciders**: Core Engineering Team
- **Date**: 2026-09-01

---

## Context

Financial ledgers require strict, uncompromised consistency. Under high concurrency, traditional relational database patterns rely on row-level locking (`SELECT ... FOR UPDATE`), distributed mutexes, or Optimistic Concurrency Control (OCC).

Under extreme transaction volumes (>10,000 TPS), row locking causes lock contention, database connection pool exhaustion, and deadlock risks. OCC results in excessive retries and degraded throughput under hot-account contention (e.g., central settlement accounts or hot wallets).

---

## Decision

We decided to adopt a **Single-Writer Event Loop Model** inspired by LMAX Disruptor and Redis single-threaded execution principles.

1. **Lock-Free State Machine**: All state mutations (account creation, credit/debit validation, balance updates) are executed strictly in a single dedicated Go goroutine event loop (`internal/engine/loop.go`).
2. **Ring Buffer Queue**: Inbound transfer submission goroutines push requests to a lock-free, bounded MPMC (Multi-Producer Multi-Consumer) ring buffer queue (`internal/engine/ring_buffer.go`).
3. **Batch Drain & Execute**: The event loop drains available requests from the queue in micro-batches, validates double-entry invariants, appends to the Write-Ahead Log (WAL), and mutates in-memory account balances sequentially.

---

## Consequences

### Positive
- **Zero Race Conditions**: Completely eliminates data races, deadlocks, and hot-account lock contention.
- **Deterministic Invariant Validation**: Enforces $\sum \text{debits} == \sum \text{credits}$ per batch with $O(1)$ in-memory lookups.
- **Predictable Latency**: Eliminates lock wait overhead and thread context-switching latency.

### Negative / Trade-Offs
- **Single Thread CPU Bound**: Processing throughput per engine instance is bounded by the processing speed of a single CPU core.
- **Memory Footprint**: Requires all active account balances to reside in memory for low-latency lookups.
