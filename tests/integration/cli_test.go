package integration

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestCLITool(t *testing.T) {
	// 1. Build the CLI binary
	cliDir := filepath.Join("..", "..", "cmd", "ledger-cli")
	cliBin := filepath.Join(t.TempDir(), "ledger-cli")

	buildCmd := exec.Command("go", "build", "-o", cliBin, ".")
	buildCmd.Dir = cliDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ledger-cli: %v\nOutput:\n%s", err, string(out))
	}

	// 2. Spin up Ledger + REST Server
	tmpDir := t.TempDir()
	w, err := wal.Open(filepath.Join(tmpDir, "wal"), 64<<20)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	cfg := engine.DefaultConfig()
	ledger, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("failed to create ledger: %v", err)
	}
	defer ledger.Close()

	restServer := api.NewRESTServer(ledger)
	ts := httptest.NewServer(restServer)
	defer ts.Close()

	// Helper to run CLI commands
	runCLI := func(args ...string) (string, error) {
		fullArgs := append([]string{"-url", ts.URL}, args...)
		cmd := exec.Command(cliBin, fullArgs...)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		err := cmd.Run()
		return out.String(), err
	}

	// 3. Test: Get health info
	out, err := runCLI("info")
	if err != nil {
		t.Fatalf("cli info failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, `"status": "UP"`) {
		t.Errorf("expected info output to contain 'UP', got: %s", out)
	}

	// 4. Test: Create Account 1
	out, err = runCLI("account", "create", "-id", "00000000000000000000000000000001", "-initial-credits", "500")
	if err != nil {
		t.Fatalf("cli account create failed: %v, output: %s", err, out)
	}
	if !strings.Contains(out, "500") {
		t.Errorf("expected create output to contain initial credits, got: %s", out)
	}

	// 5. Test: Create Account 2
	out, err = runCLI("account", "create", "-id", "00000000000000000000000000000002")
	if err != nil {
		t.Fatalf("cli account create 2 failed: %v, output: %s", err, out)
	}

	// 6. Test: Create Transfer
	out, err = runCLI("transfer", "create",
		"-id", "00000000000000000000000000000111",
		"-debit", "00000000000000000000000000000001",
		"-credit", "00000000000000000000000000000002",
		"-amount", "100",
		"-idempotency-key", "cli-test-key-1",
	)
	if err != nil {
		t.Fatalf("cli transfer create failed: %v, output: %s", err, out)
	}

	// 7. Test: Get Account 1 Balance (500 - 100 = 400)
	out, err = runCLI("account", "get", "-id", "00000000000000000000000000000001")
	if err != nil {
		t.Fatalf("cli account get failed: %v, output: %s", err, out)
	}

	var acc map[string]interface{}
	if err := json.Unmarshal([]byte(out), &acc); err != nil {
		t.Fatalf("failed to parse get account JSON output: %v", err)
	}
	if acc["balance"] != "400" {
		t.Errorf("expected account 1 balance '400', got: %v", acc["balance"])
	}
}
