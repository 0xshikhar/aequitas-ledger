# ADR-004: Primary-Follower WAL Streaming Replication & Replica Promotion

- **Status**: Accepted
- **Deciders**: Core Engineering Team
- **Date**: 2026-09-01

---

## Context

High availability requires that data is replicated off the primary node in real time. If the primary node experiences hardware failure or loses disk access, a replica node must be capable of serving read traffic or promoting to primary with zero data divergence.

---

## Decision

We built a **Primary-Follower WAL Replication Engine** (`internal/replication/`):

1. **gRPC Streaming Protocol**: The Primary server runs a replication service (`replication/server.go`) streaming WAL record chunks over gRPC to connected Follower replicas.
2. **Follower Catchup Worker**: Follower nodes (`replication/follower.go`) continuously receive stream chunks, verify LSN sequencing and CRC checksums, and apply records to their local state.
3. **Failover Promotion Primitive**: If the Primary node drops offline, a Follower node can execute `PromoteToPrimary()`, transitioning its role from `FOLLOWER` to `PRIMARY`, enabling full write capabilities.

---

## Consequences

### Positive
- **Real-Time Data Redundancy**: Keeps follower nodes within milliseconds of the Primary node's active LSN state.
- **Read Scalability**: Follower nodes serve read queries (`GetAccount`, `GetBalance`) off-loading load from the Primary single writer.
- **High-Availability Failover**: Enables active-standby failover topologies.

### Negative / Trade-Offs
- **Asynchronous Replication Lag**: Replication streams asynchronously over the network, introducing potential sub-millisecond lag.
