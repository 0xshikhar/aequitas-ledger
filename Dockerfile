FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o aequitas-server ./cmd/server/main.go

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/aequitas-server /app/aequitas-server

EXPOSE 50051 6060
ENTRYPOINT ["/app/aequitas-server"]
