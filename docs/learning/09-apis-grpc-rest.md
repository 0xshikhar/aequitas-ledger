# 09 — APIs: gRPC, REST & Middleware

**Files in focus:** `proto/ledger/v1/ledger.proto`, `internal/api/server.go`,
`internal/api/rest.go`, `internal/api/idempotency.go`

The API layer is where Go's interface composition earns its keep: handlers
that know nothing about transport, interceptors that know nothing about
handlers, and two transports over one engine. This document covers protobuf
codegen, the gRPC interceptor pattern, net/http routing, and the discipline
of keeping both surfaces consistent.

---

## 1. One engine, two transports

```
gRPC client ──► AccountsHandler ──┐
                                  ├──► engine.Ledger (single writer)
REST client ──► RESTServer ───────┘
```

Neither handler contains business logic. `internal/api/server.go` translates
proto types to `core.Transfer` and back; `internal/api/rest.go` does the
same for JSON. All decisions live in the engine — which is why the
follower-rejects-writes test can exercise `engine.Ledger` directly and the
API mappings are thin, testable layers over it.

The proto (proto3) models 128-bit values honestly:

```protobuf
message Money {
  uint64 lo = 1;
  uint64 hi = 2;
}
```

Note what this *avoids*: encoding a Uint128 as a JSON string in the binary
API (lossy round-trips through decimal parsing on the hot path) or as two
loosely-named fields. REST, being human-facing, *does* use decimal strings —
the right fidelity per surface.

Codegen (`protoc --go_out --go-grpc_out`) produces `ledger.pb.go` and
`ledger_grpc.pb.go` — generated files are never edited, and the repo's
handler implements the generated server interface
(`ledgerv1.UnimplementedLedgerServiceServer` embedded for forward
compatibility: adding an RPC later doesn't break the binary until you
implement it).

## 2. The unary interceptor pattern

gRPC middleware is a function that wraps a handler:

```go
// internal/api/idempotency.go
func IdempotencyUnaryInterceptor() grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
        handler grpc.UnaryHandler) (any, error) {

        md, ok := metadata.FromIncomingContext(ctx)
        if ok {
            if keys := md.Get("idempotency-key"); len(keys) > 0 { ... }
            ctx = ContextWithIdempotencyKey(ctx, key)
        }
        return handler(ctx, req)   // call the real handler with the enriched context
    }
}
```

Read this as decorator-as-value: `IdempotencyUnaryInterceptor()` returns the
middleware; the returned function receives the next handler in the chain.
The pattern scales to chains (logging → recovery → auth → handler) and is
the same shape as `func(http.Handler) http.Handler` in net/http — learn it
once, use it in both transports.

The context-value plumbing uses the private-key pattern
(`idempotencyKeyCtxKey{}`) so only `internal/api` can read/write this value
— document 02, §4. Note the interceptor's job ends at *placing* the key in
context; the *enforcement* lives in the engine. Middleware that enforces
business rules scatters policy across layers.

Wiring happens once in `cmd/server/main.go`:

```go
grpcServer := grpc.NewServer(grpc.UnaryInterceptor(api.IdempotencyUnaryInterceptor()))
ledgerv1.RegisterLedgerServiceServer(grpcServer, handler)
```

(Unary interceptors cover request/response RPCs; streaming RPCs would need
`grpc.StreamInterceptor` — the replication protocol is deliberately raw TCP
precisely to avoid protobuf/gRPC overhead on the hot stream.)

## 3. net/http: routing, method discipline, timeouts

```go
// internal/api/rest.go
func (s *RESTServer) registerRoutes() {
    s.mux.HandleFunc("/healthz", s.handleHealthz)
    s.mux.HandleFunc("/v1/accounts", s.handleAccounts)          // POST
    s.mux.HandleFunc("/v1/accounts/", s.handleGetAccount)       // GET /{id}
    s.mux.HandleFunc("/v1/transfers", s.handleCreateTransfer)   // POST
}
```

Go 1.22+ `ServeMux` supports method patterns (`mux.HandleFunc("POST
/v1/transfers", ...)`) which would fold the manual method checks into the
route table — the manual `if r.Method != http.MethodPost` checks are the
older, portable idiom. The trailing-slash route `/v1/accounts/` catches the
subtree, and the handler extracts the ID with
`strings.TrimPrefix(r.URL.Path, "/v1/accounts/")`.

Two details that separate toy servers from production ones, both present in
`cmd/server/main.go`:

```go
httpServer := &http.Server{
    Addr:         ":" + cfg.RESTPort,
    Handler:      restServer,
    ReadTimeout:  10 * time.Second,
    WriteTimeout: 10 * time.Second,
}
```

- **`http.Server` with timeouts.** The zero-value `http.Server` never times
  out a connection; a slow client holds a goroutine (and its stack) forever.
  Read/Write timeouts bound the damage.
- **`httptest.NewServer(restServer)` in tests** — the REST integration test
  drives the real `http.Handler` through a real socket, so JSON encoding,
  routing, and error mapping are all under test, not a mock.

JSON decoding uses the strict-enough default (`json.NewDecoder(r.Body)`),
with errors mapped to 400 before any engine call. Response encoding funnels
through `writeJSON`/`writeError` helpers so Content-Type and status handling
can't drift per-handler.

## 4. Keeping two surfaces honest

The same request arrives as JSON and as protobuf. The translation layers
must agree — on parsing (C0.12: both `parseHexID` and `bytesToID` were
fixed in the same change to require exactly 16 bytes) and on error semantics
(C0.11: both map `ErrNotLeader` to 503/Unavailable). The pattern that keeps
them honest:

1. One mapping table (document 06, §2) as the spec.
2. Proof tests per surface asserting the same scenario yields the
   equivalent response (`TestRESTFollowerReturnsServiceUnavailableOnWrites`
   ↔ `TestFollowerModeRejectsWrites`).
3. When adding an RPC or route, port *both* the parsing strictness and the
   error mapping — the bug class is always "one surface forgot."

## 5. Testing gRPC without sockets: bufconn

```go
// tests/integration/transfers_test.go
lis := bufconn.Listen(bufSize)                    // in-memory listener
grpcServer := grpc.NewServer(grpc.UnaryInterceptor(...))
go grpcServer.Serve(lis)
conn, _ := grpc.NewClient("passthrough:///bufnet",
    grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
        return lis.Dial()
    }), ...)
```

`bufconn` swaps the TCP listener for an in-memory pipe: real gRPC stack,
real interceptors, real serialization — no ports, no flaky binds, no TLS
setup. Combined with `t.TempDir()`-backed WAL storage, the integration tests
are hermetic: no external state, safe under `-race`, fast enough to run on
every save.

> **Exercise 1.** Add `GET /v1/accounts/{id}` support for the `0x` prefix
> being optional vs required. Then decide: is accepting both leniency (good
> UX) or a trust-boundary leak (document 06, §3)? Write the test that
> enforces whichever you decide.
>
> **Exercise 2.** Add a logging interceptor that wraps the idempotency one
> (order matters: which should run first?). Use `info.FullMethod` and log
> duration + result code without leaking the idempotency key.
>
> **Exercise 3.** The REST layer has no idempotency middleware — the key
> comes from header *or* body in `handleCreateTransfer`. Compare the two
> approaches and argue which belongs where, given that gRPC has no body
> equivalent.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| Thin transport, fat engine | both handlers | Translation only; policy lives in the engine |
| Interceptor = decorator value | `IdempotencyUnaryInterceptor` | `func(ctx, req, info, handler)` chains compose |
| Private context keys | `idempotencyKeyCtxKey{}` | Zero-size unexported structs; no collisions |
| Configured `http.Server` | `cmd/server/main.go` | Zero-value Server has no timeouts |
| bufconn | gRPC tests | Real stack, in-memory transport |
| Two surfaces, one spec | error mapping tables | Port parsing + mapping to both, same commit |
