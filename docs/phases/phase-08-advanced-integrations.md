# Phase 8 — REST API Gateway & Transactional Event Outbox Deep-Dive

## 1. Objective & Problem Statement

While high-throughput internal microservices interact with **Aequitas Ledger** over gRPC, web dashboards, front-end administrative portals, third-party payment gateways, and asynchronous analytics pipelines require standard HTTP/JSON REST interfaces and event streaming.

Phase 8 provides:
1. **Lossless REST API Gateway (`internal/api/rest.go`)**: Dual HTTP/JSON REST API running on port `:8080` alongside the gRPC server.
2. **Lossless Decimal & Binary Marshaling**: Safe translation between JSON string representations (`"amount": "100.50"`) and internal 128-bit money primitives (`core.Uint128`) to prevent IEEE 754 floating point precision loss.
3. **Transactional Event Outbox Publisher (`internal/events/publisher.go`)**: Pub/Sub memory event broadcaster delivering real-time `transfer.processed` events in exact LSN commit order.
4. **End-to-End REST Integration Suite (`tests/integration/rest_test.go`)**.

---

## 2. Core Go Language Concepts & Engineering Internals

### A. Go Standard Library `net/http.ServeMux` Router
Instead of bloating the codebase with heavy third-party web frameworks, the REST Gateway leverages Go's built-in `net/http.ServeMux`:

```go
type RESTServer struct {
	ledger *engine.Ledger
	mux    *http.ServeMux
}

func (s *RESTServer) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/v1/accounts", s.handleAccounts)
	s.mux.HandleFunc("/v1/accounts/", s.handleGetAccount)
	s.mux.HandleFunc("/v1/transfers", s.handleCreateTransfer)
}
```

* **Go Concept (`http.Handler` Interface)**: Implementing `ServeHTTP(w http.ResponseWriter, r *http.Request)` allows `RESTServer` to satisfy standard HTTP interfaces, making it compatible with `httptest.NewServer` for zero-network integration testing.

---

### B. Lossless String-to-Uint128 JSON Parsing
Standard JSON decoders parse numbers as 64-bit floating point numbers (`float64`), which corrupts large financial values. To prevent loss of precision:

```go
type createTransferReq struct {
	ID              string `json:"id"`
	DebitAccountID  string `json:"debit_account_id"`
	CreditAccountID string `json:"credit_account_id"`
	Amount          string `json:"amount"`
	IdempotencyKey  string `json:"idempotency_key"`
}
```

When processing the request:
```go
amount, err := core.FromString(req.Amount)
```
Strings are converted directly into 128-bit unsigned integer base units using arbitrary-precision decimal parsing, guaranteeing **0% precision loss**.

---

### C. Thread-Safe Event Broadcaster (`sync.RWMutex`)
The `Publisher` uses a read-write mutex to manage dynamic subscriber registrations while broadcasting events without holding lock contention during delivery:

```go
type Publisher struct {
	mu          sync.RWMutex
	subscribers []Subscriber
}

func (p *Publisher) Publish(ev Event) {
	p.mu.RLock()
	subs := make([]Subscriber, len(p.subscribers))
	copy(subs, p.subscribers)
	p.mu.RUnlock()

	for _, sub := range subs {
		sub(ev)
	}
}
```

* **Go Concept (Read-Lock Copy & Release)**: Copying subscriber references inside `RLock()` and releasing the lock *before* invoking callbacks prevents subscriber deadlocks if a callback attempts to subscribe/unsubscribe mid-event.

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **HTTP Framework** | Native `net/http` | Gin / Fiber / Echo | Standard library `net/http` has zero external dependencies, minimal memory footprint, and guaranteed backward compatibility. |
| **JSON Numeric Types** | String Decimal Decoding | Standard JSON Numbers | Floating-point JSON numbers lose precision beyond 15 digits; string decimals preserve absolute 128-bit precision. |
| **Event Broadcast** | Non-blocking Copy Pub/Sub | Shared Mutex Callback Lock | Invoking callbacks while holding a mutex causes deadlocks if a subscriber blocks; copying the subscriber slice under `RLock` ensures lock-free event delivery. |
