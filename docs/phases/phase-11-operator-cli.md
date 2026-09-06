# Phase 11 — Interactive Operator CLI Tool

The **Aequitas Ledger** exposes a high-throughput gRPC and REST interface for systems integration. However, in production environments, operations teams (SREs, platform engineers, and support staff) need an ergonomic way to query the ledger, provision administrative accounts, and interact with the system without writing manual `curl` commands or custom scripts.

In Phase 11, we built an **Interactive Operator CLI Tool** (`ledger-cli`) to solve this problem.

---

## 1. Ergonomics and Tooling

We chose to implement `ledger-cli` using the standard library `flag` and `net/http` packages for zero-dependency purity, matching the project's philosophy.

### Command Structure

The CLI is structured around subcommands:
- `ledger-cli info`: Fetches the system's operational status and checks connectivity.
- `ledger-cli account create`: Provisions a new double-entry account.
- `ledger-cli account get`: Retrieves the current derived balance and account metadata.
- `ledger-cli transfer create`: Manually injects a double-entry journal transfer.

Using `flag.NewFlagSet("subcommand", flag.ExitOnError)`, we isolate flag parsing for each command. This ensures that `account get` flags do not leak into `account create`.

---

## 2. API Communication

The CLI relies heavily on the `net/http` package. We built helper methods (`doGet`, `doPost`) to abstract away the boilerplate of JSON marshaling, request execution, and response reading. 

```go
func doPost(client *http.Client, path string, payload interface{}) {
	b, _ := json.Marshal(payload)
	url := strings.TrimRight(*apiURL, "/") + path
	resp, _ := client.Post(url, "application/json", bytes.NewReader(b))
	defer resp.Body.Close()
	printResponse(resp)
}
```

This pattern provides a single point of failure handling. If the API endpoint changes or we need to add global headers (like Authentication tokens in the future), we only need to update these helper functions.

---

## 3. The Value of the CLI for Operations

The operator CLI serves several critical functions:
1. **Disaster Recovery**: If an automated system fails to post an entry, an operator can manually push the transfer via `ledger-cli transfer create`.
2. **System Health Verification**: `ledger-cli info` serves as an immediate connectivity check against the `/healthz` endpoint, proving that the HTTP REST port is accessible.
3. **Audit and Investigation**: The `account get` command provides a rapid lookup for balances when investigating support escalations.

---

## 4. End-to-End Integration Testing

To guarantee the CLI remains functional as the API evolves, we implemented `tests/integration/cli_test.go`. 

This test uses the powerful pattern of compiling the binary on the fly and executing it against a live test server:
```go
// Compile the binary
buildCmd := exec.Command("go", "build", "-o", cliBin, ".")
buildCmd.Run()

// Spin up a test API server
ts := httptest.NewServer(api.NewRESTServer(ledger))

// Execute the CLI against the test server
cmd := exec.Command(cliBin, "-url", ts.URL, "info")
cmd.Run()
```
This verifies that the CLI arguments are parsed correctly and the resulting JSON payloads match the API's expectations.

## What's Next?
The CLI is currently bound to the REST API. In future extensions, it could be pointed at the gRPC endpoint for higher performance using protocol buffers, or expanded with a `--watch` flag to stream ledger events in real-time.
