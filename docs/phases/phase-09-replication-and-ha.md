# Phase 9 — Primary-Replica WAL Stream Replication & Read-Scalability Deep-Dive

## 1. Objective & Problem Statement

In mission-critical financial applications, running a single node creates a single point of failure (SPOF) and limits read throughput. Read queries (such as customer balance checks or audit reporting) can saturate CPU bandwidth on the single-writer Primary engine.

Phase 9 introduces **Active-Passive Primary-Replica Stream Replication**:
1. **Primary WAL Streaming Server (`internal/replication/server.go`)**: Streams durably committed log segments in real time over TCP sockets.
2. **Follower Catchup Worker Daemon (`internal/replication/follower.go`)**: Connects to the Primary, streams incoming WAL frames, writes local WAL copies, and updates a read-only `AccountManager`.
3. **Horizontal Read Scalability**: Follower nodes serve read queries (`GetAccount`, `GetBalance`) with zero CPU or lock contention on the Primary engine loop.
4. **Double-Entry Invariant Integrity**: Replicas maintain identical balance invariants:
$$\sum \text{PostedDebits}_{\text{Follower}} = \sum \text{PostedCredits}_{\text{Follower}}$$

---

## 2. Core Go Language Concepts & Engineering Internals

### A. TCP Socket Streaming & Wire Framing
The Primary streams binary log records over TCP using framed network protocol units:

$$\text{[LSN: 8B]} \;\Vert\; \text{[RecordType: 1B]} \;\Vert\; \text{[PayloadLen: 4B]} \;\Vert\; \text{[Payload: NB]}$$

```go
func (s *Server) handleConn(conn net.Conn) {
    // 1. Read requested LSN (8 bytes)
    var reqLSNBuf [8]byte
    io.ReadFull(conn, reqLSNBuf[:])
    startLSN := int64(binary.BigEndian.Uint64(reqLSNBuf[:]))

    // 2. Stream WAL records
    s.wal.Recover(func(r wal.Record) error {
        // Frame bytes: [LSN:8B] [Type:1B] [Len:4B] [Payload:NB]
        buf := make([]byte, 8+1+4+len(r.Payload))
        binary.BigEndian.PutUint64(buf[0:8], uint64(currentLSN))
        buf[8] = byte(r.Type)
        binary.BigEndian.PutUint32(buf[9:13], uint32(len(r.Payload)))
        copy(buf[13:], r.Payload)
        conn.Write(buf)
        return nil
    })
}
```

* **Go Concept (`io.ReadFull`)**: Prevents short network read bugs by guaranteeing that exactly 13 header bytes are buffered before parsing binary length headers.

---

### B. Reader-Writer Lock Concurrency (`sync.RWMutex`) on Replicas
Follower nodes use a `sync.RWMutex` to separate incoming stream writes from concurrent client read queries:

```go
func (f *Follower) GetAccount(id [16]byte) (core.Account, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    acc, err := f.accounts.Get(id)
    if err != nil {
        return core.Account{}, err
    }
    return *acc, nil
}
```

* **Concurrent Reads**: Multiple API clients calling `GetAccount` hold shared `RLock()`, executing reads in parallel without blocking each other. The replication worker acquires an exclusive `Lock()` only for a few microseconds while applying the latest streaming transfer batch.

---

### C. Automatic Reconnection & LSN Resynchronization Loop
If network connectivity drops between Primary and Follower:
```go
func (f *Follower) syncLoop() error {
    conn, err := net.Dial("tcp", f.primaryAddr)
    if err != nil {
        return err
    }
    defer conn.Close()

    // Send last known LSN + 1 to resume stream seamlessly
    var reqBuf [8]byte
    binary.BigEndian.PutUint64(reqBuf[:], uint64(f.lastLSN+1))
    conn.Write(reqBuf[:])
    // Stream frames continuously...
}
```
* **Resilience**: The Follower automatically reconnects, transmits its `lastLSN + 1`, and resumes streaming from the exact missing frame offset without losing data or requiring full state resync.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **Replication Mode** | Asynchronous WAL Streaming | Synchronous 2PC / Multi-Paxos | Synchronous replication introduces network RTT latency into every transfer commit; async streaming maintains high Primary write throughput while scaling read replicas. |
| **Transport Protocol** | Raw Binary TCP Socket | gRPC Server Streaming | Raw TCP streaming minimizes framing overhead and CPU allocations during high-frequency log replication. |
| **Replica State** | In-Memory Memory View + Local WAL | Shared Disk Mount | Direct memory replication avoids shared disk SAN/NFS lock contention and provides instant sub-millisecond query performance. |
