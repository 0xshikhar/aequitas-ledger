## aequitas-ledger developer workflows.
## CI mirrors these targets (see .github/workflows/ci.yml) — keep both in sync.

GO ?= go

.PHONY: help proto-gen build vet fmt lint test race cover bench bench-smoke panic-gate tidy check clean

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

proto-gen: ## Regenerate protobuf stubs (requires protoc + protoc-gen-go + protoc-gen-go-grpc)
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/ledger/v1/ledger.proto

build: ## Compile all packages
	$(GO) build ./...

vet: ## go vet
	$(GO) vet ./...

fmt: ## Fail if any file needs gofmt
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

lint: ## golangci-lint (install: brew install golangci-lint)
	golangci-lint run --timeout 5m

test: ## Full test suite
	$(GO) test ./...

race: ## Full test suite under the race detector (same as CI)
	$(GO) test -race ./...

cover: ## Coverage report
	$(GO) test -race -covermode=atomic -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out

bench: ## All benchmarks (1s each)
	$(GO) test -run '^$$' -bench . -benchmem ./bench/... ./tests/...

bench-smoke: ## Assert catastrophic throughput regressions only (see scripts/bench_smoke.sh)
	./scripts/bench_smoke.sh

panic-gate: ## No panic() in internal/ except the documented invariant panic (C0.7)
	@hits=$$(grep -RnE '\bpanic\(' internal --include='*.go' | grep -v 'internal/core/account.go' || true); \
	if [ -n "$$hits" ]; then echo "panic() found:"; echo "$$hits"; exit 1; fi; \
	echo "panic gate ok"

tidy: ## go mod tidy — must produce no diff
	$(GO) mod tidy
	@git diff --exit-code go.mod go.sum || (echo "go.mod/go.sum changed; commit the result"; exit 1)

check: fmt vet panic-gate race ## Everything CI runs before lint

clean: ## Remove build/test artifacts
	rm -f coverage.out
	$(GO) clean -testcache
