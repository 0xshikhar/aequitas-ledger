FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/aequitas-server ./cmd/server/main.go

FROM alpine:3.19

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
COPY --from=builder /app/aequitas-server /app/aequitas-server

RUN mkdir -p /var/lib/aequitas/wal && chown -R appuser:appgroup /var/lib/aequitas

USER appuser

EXPOSE 50051 8080 6060
ENTRYPOINT ["/app/aequitas-server"]
