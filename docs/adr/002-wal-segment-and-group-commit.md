# ADR-002: Write-Ahead Log (WAL) Durability and Group Commit Model

- **Status**: Accepted
- **Deciders**: Core Engineering Team
- **Date**: 2026-09-01

---

## Context

Financial transactions must survive hardware crashes, power outages, and sudden process terminations. Disk I/O, specifically synchronous disk flushes (`fsync()`), is the slowest operation in database engine design. 

Invoking `fsync()` for every individual transfer caps throughput at disk IOPS limits (typically 100–500 TPS on standard SSDs).

---

## Decision

We designed a custom append-only **Write-Ahead Log (WAL)** layer (`internal/wal/`) featuring:

1. **Fixed-Size Segment Files**: Log records are appended to pre-allocated segment files (`segment-00000000.wal`).
2. **CRC32 Checksum Protection**: Every WAL record payload is guarded by an inline CRC32 checksum (`[type|len|payload|crc32]`) for corruption detection.
3. **Group Commit Primitive**: Rather than flushing disk for every transfer, the single-writer loop aggregates inbound transfers into micro-batches, writes the batch to the active WAL segment, and executes a single `Sync()` per batch.

---

## Consequences

### Positive
- **High Throughput**: Amortizes disk flush overhead across hundreds or thousands of transactions per batch.
- **Crash Safety**: Guarantees zero committed transaction data loss upon hard power loss.
- **Tail Corruption Handling**: Recovery replay detects partial/truncated writes at log tail during restart and truncates safely.

### Negative / Trade-Offs
- **Batch Latency Tuning**: Requires careful tuning of maximum batch size and drain timeouts to balance latency vs throughput.
