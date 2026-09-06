# Phase 4 — gRPC API Surface & Middleware Deep-Dive

## 1. Objective & Problem Statement

To expose the high-throughput engine to external microservices (such as payment gateways, exchanges, or billing services), **Aequitas Ledger** provides a gRPC API surface defined in `proto/ledger/v1/ledger.proto`.

Key operational guarantees:
1. **gRPC Protocol Buffers**: High-performance HTTP/2 binary transport with zero string parsing overhead.
2. **Idempotency Interceptor Middleware**: Automatic extraction of idempotency keys from HTTP/2 metadata headers (`Idempotency-Key`), guaranteeing duplicate protection at the API gateway layer.
3. **Type Conversions**: Safe conversion between Protobuf message formats and internal core domain primitives (`Uint128`, `[16]byte` IDs).

---

## 2. Core Go Language Concepts & Engineering Internals

### A. Protobuf Service Definition (`ledger.proto`)
```protobuf
syntax = "proto3";
package ledger.v1;

service LedgerService {
  rpc CreateAccount (CreateAccountRequest) returns (CreateAccountResponse);
  rpc GetAccount (GetAccountRequest) returns (GetAccountResponse);
  rpc CreateTransfer (CreateTransferRequest) returns (CreateTransferResponse);
}
```

---

### B. Idempotency Unary Interceptor
gRPC interceptors wrap RPC handler execution (similar to HTTP middleware):

```go
func IdempotencyUnaryInterceptor() grpc.UnaryServerInterceptor {
    return func(
        ctx context.Context,
        req any,
        info *grpc.UnaryServerInfo,
        handler grpc.UnaryHandler,
    ) (any, error) {
        md, ok := metadata.FromIncomingContext(ctx)
        if ok {
            keys := md.Get("idempotency-key")
            if len(keys) > 0 && keys[0] != "" {
                keyBytes := sha256.Sum256([]byte(keys[0]))
                ctx = ContextWithIdempotencyKey(ctx, keyBytes)
            }
        }
        return handler(ctx, req)
    }
}
```

* **Go Concept (`context.Context` Value Propagation)**: `ContextWithIdempotencyKey` attaches the parsed 32-byte SHA-256 key to the RPC request context. The down-stream handler retrieves the key using `IdempotencyKeyFromContext(ctx)` without polluting function parameters.

---

### C. Handler Status Mapping
Domain errors in Go (`core.ErrInsufficientFunds`, `core.ErrAccountNotFound`) are translated into standard gRPC status codes:

```go
res, err := h.ledger.CreateTransfer(ctx, tr)
if err != nil {
    var notFound core.ErrAccountNotFound
    if errors.As(err, &notFound) {
        return nil, status.Errorf(codes.NotFound, "%v", err)
    }
    var insufficient core.ErrInsufficientFunds
    if errors.As(err, &insufficient) {
        return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
    }
    return nil, status.Errorf(codes.Internal, "transfer execution failed: %v", err)
}
```

---

## 3. Alternative Choices & Architectural Trade-offs

| Design Choice | Chosen Approach | Alternative Evaluated | Trade-off / Justification |
|---|---|---|---|
| **API Transport** | gRPC (HTTP/2 + Proto3) | REST / JSON over HTTP/1.1 | gRPC uses binary HTTP/2 multiplexing, reducing payload serialization and connection overhead by over 60%. |
| **Idempotency Header** | Metadata Interceptor | Manual Handler Boilerplate | Interceptor centralizes header parsing across all transfer RPC methods, eliminating code duplication. |
