# 11 — Modules, Project Layout & Dependency Hygiene

**Files in focus:** `go.mod`, the repository tree, `cmd/server/main.go`,
`cmd/ledger-cli/main.go`

How a Go repo is arranged determines what can import what, what gets
recompiled, and — more subtly — what the *reviewer* believes about the
project's discipline. This document covers module mechanics and the layout
decisions in this repo, including one cautionary tale about a dependency
that sneaked in through an unrelated command.

---

## 1. The tree, and why

```
aequitas-ledger/
├── cmd/
│   ├── server/main.go        # one main per binary
│   ├── ledger-cli/main.go    # operator CLI
│   └── bench/main.go
├── internal/                 # compiler-enforced privacy
│   ├── core/                 # domain types — imports NOTHING internal
│   ├── wal/                  # durability — imports core
│   ├── engine/               # state machine — imports core, wal, observability
│   ├── snapshot/ replication/ events/ api/ observability/ config/ store/
├── proto/ledger/v1/          # generated code, never edited
├── tests/{unit,integration}/ # external test packages
├── bench/                    # cross-implementation benchmark harness
└── deploy/                   # k8s, prometheus
```

Two rules do the heavy lifting:

- **`internal/` is compiler-enforced.** Nothing outside
  `aequitas-ledger/...` can import `internal/...`; the toolchain rejects it.
  Your API surface to the world is exactly what you export outside
  `internal/` — for a service binary, effectively nothing.
- **Dependency direction points downward and never cycles.** `core` imports
  nothing from the project (it is the vocabulary); `wal` depends on `core`;
  `engine` orchestrates both; `api` depends on `engine`; `cmd` depends on
  everything. `go list -deps` or a failed build catches cycles — but the
  real discipline is refusing the tempting "just one small import" that
  would let `core` know about `engine`. Layering is a design constraint you
  enforce in review, not a tool.

Within `internal/`, packages are grouped by *responsibility* (wal, engine,
api), not by *technical layer* (models/, utils/, helpers/). The
`helpers.go`/`utils` pattern is a smell: if you can't name what a package
*is*, it will accrete everything.

## 2. go.mod mechanics

```
module aequitas-ledger

go 1.25.0

require (
    github.com/jackc/pgx/v5 v5.10.0
    github.com/prometheus/client_golang v1.24.1
    google.golang.org/grpc v1.83.0
    ...
)
```

- The **module path** is the import prefix for every package in the repo.
  Pick a name you'd be happy to publish; renaming later means rewriting
  every import.
- The **`go` directive** sets the minimum toolchain and gates language
  features (loop-variable semantics changed at 1.22 — `for _, x := range`
  now gives a fresh `x` per iteration; this repo relies on it in every
  `go func(){ ... x ... }` closure).
- **`go.sum`** pins content hashes. It is not optional and not "cache" —
  commit it, verify it, and treat its diffs in review as supply-chain
  review (every new hash is a new blob of third-party code you now trust).
- **`go mod tidy`** reconciles the module graph with actual imports. Run it
  and *expect no diff* on a healthy tree.

## 3. The cautionary tale: a dependency nobody asked for

`golang.org/x/net/websocket` appeared in `go.mod` — because an
unrelated `rpc-test` command (Ethereum JSON-RPC probing, hardcoded
third-party endpoints) was added to `cmd/ledger-cli/main.go`. Nothing about
a ledger needs a websocket client; now `go.sum` trusts an extra module, the
binary pulls more of the dependency graph than it uses, and a reviewer
wondering "why does the ledger CLI dial ethereum-mainnet over wss?" finds
no answer.

The cleanup (C0.10) is a case study in dependency hygiene:

1. Delete the feature, not just the import.
2. `go mod tidy` — the dependency falls out of `go.mod`/`go.sum` only when
   no package imports it.
3. Review `go.sum` diffs as carefully as code diffs.

The deeper rule: **dependencies are architecture.** Every `require` is a
component you will upgrade, audit, and carry — so the bar for adding one is
the same as the bar for accepting a design. (This repo's direct
dependencies: grpc, protobuf, pgx — for the Postgres *baseline*, not the
server path — and prometheus. That is a deliberately short list.)

## 4. `go vet`, gofmt, and the toolchain as reviewer

```sh
gofmt -l .          # formatting drift — should print nothing
go vet ./...        # printf verbs, lock copies, unreachable code
go build ./...      # everything compiles, including tests' deps
```

`gofmt` is non-negotiable in Go culture — diffs contain zero formatting
noise, so review is pure semantics. (`gofmt -w internal/engine/event.go`
after writing it; editors do this on save.) `go vet` catches real bug
classes: a `fmt.Errorf` with mismatched verbs, copying a struct that
contains a `sync.Mutex`, loop-variable capture mistakes pre-1.22. Neither
replaces tests; both belong in CI before any test runs (the roadmap's
P5.1/P5.2 — a Makefile with `proto lint vet test race cover` targets is the
pending mechanical work).

## 5. Version-control hygiene as engineering signal

This repo's own history supplied the lesson: at one point the *entire*
`docs/` tree — ADRs, the benchmark report, this learning series — was
untracked while `Status.md` claimed "ADRs complete and committed." The
statement was true of the filesystem and false of the repository. A few
habits keep the two identical:

- Small, frequent commits with conventional messages
  (`fix(engine): ...`, `feat(wal): ...` — the existing history follows this).
- `git status` clean before declaring done; untracked ≠ not-my-problem.
- One logical change per commit so a bisect lands on truth.
- Generated code committed *with* the tool version recorded (the protoc
  invocation belongs in the Makefile), so regeneration is reproducible.

> **Exercise 1.** Run `go mod graph | grep x/net` before and after removing
> the `rpc-test` command. Watch the dependency subtree shrink — then
> explain to a colleague what `x/net/websocket` could have pulled in.
>
> **Exercise 2.** Try importing `aequitas-ledger/internal/engine` from a
> scratch module outside the repo. Read the compiler error. Now you have
> felt `internal/` enforcement.
>
> **Exercise 3.** Write the Makefile: `proto`, `lint`, `vet`, `test-race`,
> `bench`, `cover` targets. If a target needs more than one command, that
> is fine — the point is that *nobody types the long version again*.

## Recap

| Concept | Where | Rule of thumb |
|---|---|---|
| `internal/` privacy | whole tree | Compiler-enforced API boundary; keep it |
| Dependency direction | core ← wal ← engine ← api ← cmd | Layers point down; cycles are design failures |
| Package by responsibility | wal/engine/api, no `utils/` | Name what a package *is*, not what it *contains* |
| go.sum review | dependency upgrades | Every hash is trusted code — diff it |
| tidy with no diff | C0.10 story | A dependency that survives `tidy` is a decision, not an accident |
| gofmt + vet in CI | pending P5.1 | Zero formatting noise; vet before tests |
| Clean VCS state | docs/ story | "Committed" means the repository, not the filesystem |
