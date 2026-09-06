# Phase 12 — Containerization & Kubernetes Production Deployment Specs

Deploying a stateful, high-throughput financial system like **Aequitas Ledger** in containerized environments introduces specific architectural requirements. Unlike stateless web applications that can scale horizontally using standard Kubernetes `Deployments`, single-writer WAL systems require durable storage guarantees, deterministic pod identities, and non-root security boundaries.

In Phase 12, we built production-ready containerization and orchestration manifests for both **Docker Compose** and **Kubernetes**.

---

## 1. Container Hardening with Multi-Stage Builds

A container image for financial software must adhere to the **least privilege principle** and minimize attack vectors.

### Key Practices Implemented:
1. **Multi-Stage Build**: Compilation occurs in `golang:1.25-alpine`, producing a statically linked binary (`CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s"`). The final runner image uses `alpine:3.19`, eliminating compiler toolchains and unnecessary shell dependencies.
2. **Non-Root Execution**: We create an unprivileged user and group (`appuser:appgroup`) in the container:
   ```dockerfile
   RUN addgroup -S appgroup && adduser -S appuser -G appgroup
   USER appuser
   ```
3. **Explicit Volume Ownership**: The WAL data directory (`/var/lib/aequitas/wal`) is explicitly assigned to `appuser` before dropping privileges.

---

## 2. Docker Compose Topology for Replication

Our `docker-compose.yml` configures a complete local production topology:
- **`ledger-primary`**: Operates in `PRIMARY` mode with a persistent volume (`primary-wal-data`). Exposes gRPC (`50051`), REST Gateway (`8080`), and Prometheus metrics (`6060`).
- **`ledger-follower`**: Operates in `FOLLOWER` mode. Waits for the primary container to pass health checks (`service_healthy`) before initiating WAL stream replication over gRPC.
- **`prometheus` & `grafana`**: Scrape and visualize engine performance (TPS, WAL sync duration, and queue depth).
- **`postgres`**: Runs the relational database used for micro-benchmark comparison testing.

---

## 3. Kubernetes Orchestration Architecture

Stateful systems require specialized Kubernetes primitives:

```
┌─────────────────────────────────────────────────────────────┐
│                   Kubernetes Cluster                        │
│                                                             │
│  ┌─────────────────────────┐     ┌───────────────────────┐  │
│  │ StatefulSet (Primary)   │     │ Deployment (Follower) │  │
│  │  Pod: aequitas-primary-0│     │  Pod: follower-1      │  │
│  │  PVC: 10Gi WAL Storage  │◄────┼──Pod: follower-2      │  │
│  └─────────────────────────┘     └───────────────────────┘  │
│               ▲                             ▲               │
│               │                             │               │
│      ┌────────┴─────────────────────────────┴────────┐      │
│      │ ClusterIP Service (aequitas-ledger-service)    │      │
│      └───────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### StatefulSet for Primary Node
Stateless `Deployments` can destroy and recreate pods on arbitrary nodes without guaranteeing volume attachment order. For the WAL-backed Primary node, we use a `StatefulSet`:
- **Deterministic Hostname**: `aequitas-primary-0`.
- **PersistentVolumeClaim (PVC)**: Dynamically provisions high-speed storage (`ReadWriteOnce`, `10Gi`) bound to the pod across restarts.
- **Headless Service**: `aequitas-primary-headless` exposes direct pod DNS for internal follower stream replication.

### Read-Only Follower Deployments
Because Followers do not process write transfers directly, they can scale horizontally using a standard Kubernetes `Deployment`. They connect to the primary node using its headless service address (`PRIMARY_ADDR=aequitas-primary-headless:50051`).

### Probes & Operational Readiness
Both primary and follower pods define `livenessProbe` and `readinessProbe` checking the HTTP REST `/healthz` endpoint:
```yaml
readinessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 2
  periodSeconds: 5
```
If a pod crashes or fails WAL recovery at boot, Kubernetes automatically removes it from the service load balancer until it recovers.
