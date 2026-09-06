# ADR-003: Asynchronous Copy-on-Write (CoW) In-Memory Snapshotting

- **Status**: Accepted
- **Deciders**: Core Engineering Team
- **Date**: 2026-09-01

---

## Context

As the WAL grows over weeks of operation, replaying millions of log records from `LSN=0` upon process restart becomes unacceptably slow (taking minutes or hours). 

Periodic snapshotting truncates WAL replay requirements by checkpointing state. However, taking a snapshot must not freeze the active transaction processing engine.

---

## Decision

We implemented an **Asynchronous Copy-on-Write (CoW) Checkpointer** (`internal/snapshot/`):

1. **In-Memory State Copy**: When a snapshot interval triggers, the single-writer event loop takes a fast point-in-time reference copy of the accounts map.
2. **Background Persistence**: The snapshot file (`snapshot-<LSN>.snap`) is written to disk in a separate background goroutine, streaming binary serialized accounts and a final SHA-256 integrity checksum.
3. **Fast Startup Recovery**: On startup, the engine loads the latest valid snapshot file and replays only the delta WAL segments created after `snapshot.LSN`.

---

## Consequences

### Positive
- **Sub-Second Recovery**: Reduces engine startup time from linear $O(N_{\text{total\_records}})$ to $O(N_{\text{delta\_records\_since\_snapshot}})$.
- **Non-Blocking Writer**: The main transaction processing loop continues executing transactions without disk write pause.

### Negative / Trade-Offs
- **Transient Memory Usage**: Point-in-time state cloning temporarily increases RAM consumption during snapshot execution.
