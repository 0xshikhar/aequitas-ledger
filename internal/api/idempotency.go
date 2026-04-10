package api

import (
	"context"
	"crypto/sha256"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type idempotencyKeyCtxKey struct{}

// ContextWithIdempotencyKey places a 32-byte idempotency key into context.
func ContextWithIdempotencyKey(ctx context.Context, key [32]byte) context.Context {
	return context.WithValue(ctx, idempotencyKeyCtxKey{}, key)
}

// IdempotencyKeyFromContext retrieves a 32-byte idempotency key from context if present.
func IdempotencyKeyFromContext(ctx context.Context) ([32]byte, bool) {
	val, ok := ctx.Value(idempotencyKeyCtxKey{}).([32]byte)
	return val, ok
}

// IdempotencyUnaryInterceptor checks incoming gRPC metadata for "idempotency-key" header.
// If provided as raw string/bytes, it hashes it via SHA-256 to produce a [32]byte key.
func IdempotencyUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			var rawKey string
			if keys := md.Get("idempotency-key"); len(keys) > 0 {
				rawKey = keys[0]
			} else if keys := md.Get("Idempotency-Key"); len(keys) > 0 {
				rawKey = keys[0]
			}

			if rawKey != "" {
				keyBytes := []byte(rawKey)
				var key [32]byte
				if len(keyBytes) == 32 {
					copy(key[:], keyBytes)
				} else {
					key = sha256.Sum256(keyBytes)
				}
				ctx = ContextWithIdempotencyKey(ctx, key)
			}
		}
		return handler(ctx, req)
	}
}
