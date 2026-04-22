package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
	ledgerv1 "aequitas-ledger/proto/ledger/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func grpcStatusEqual(t *testing.T, err error, want codes.Code, what string) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("%s: got %v (%s), want %s", what, err, status.Code(err), want)
	}
}

// C0.11: the REST gateway must map ErrNotLeader to 503 Service Unavailable,
// matching the gRPC handlers' codes.Unavailable mapping, instead of a generic
// 500. Reads on a follower must still be served (404 for unknown accounts, not
// 503), because followers are read-only, not unavailable.
func TestRESTFollowerReturnsServiceUnavailableOnWrites(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	cfg := engine.DefaultConfig()
	cfg.IsFollower = true
	l, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ts := httptest.NewServer(api.NewRESTServer(l))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/accounts", "application/json",
		strings.NewReader(`{"id":"00000000000000000000000000000001","currency":"USD"}`))
	if err != nil {
		t.Fatalf("post accounts: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("POST /v1/accounts on follower: got %d, want 503 (body: %s)", resp.StatusCode, body)
	}
	if !strings.Contains(strings.ToLower(string(body)), "follower") {
		t.Fatalf("503 body should surface the not-leader reason, got: %s", body)
	}

	transferBody := `{"id":"0000000000000000000000000000000a","debit_account_id":"00000000000000000000000000000001","credit_account_id":"00000000000000000000000000000002","amount":"5"}`
	resp, err = http.Post(ts.URL+"/v1/transfers", "application/json", strings.NewReader(transferBody))
	if err != nil {
		t.Fatalf("post transfers: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("POST /v1/transfers on follower: got %d, want 503 (body: %s)", resp.StatusCode, body)
	}

	resp, err = http.Get(ts.URL + "/v1/accounts/00000000000000000000000000000001")
	if err != nil {
		t.Fatalf("get account: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /v1/accounts/{id} on follower: got %d, want 404 (reads are served, not 503)", resp.StatusCode)
	}
}

// C0.12: IDs shorter than 16 bytes (32 hex chars) must be rejected instead of
// silently left-padded, which aliased e.g. "01" and
// "00000000000000000000000000000001" to the same account without the user
// knowing.
func TestRESTRejectsShortIDs(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	cfg := engine.DefaultConfig()
	l, err := engine.NewLedger(cfg, w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ts := httptest.NewServer(api.NewRESTServer(l))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/accounts", "application/json",
		strings.NewReader(`{"id":"01","currency":"USD"}`))
	if err != nil {
		t.Fatalf("post short-id account: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /v1/accounts with 1-byte id: got %d, want 400", resp.StatusCode)
	}

	resp, err = http.Post(ts.URL+"/v1/accounts", "application/json",
		strings.NewReader(`{"id":"0x00000000000000000000000000000001","currency":"USD"}`))
	if err != nil {
		t.Fatalf("post full-id account: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/accounts with 0x-prefixed 16-byte id: got %d, want 201", resp.StatusCode)
	}

	// The short form that previously aliased to the full ID now fails to
	// resolve instead of silently matching a different account.
	resp, err = http.Get(ts.URL + "/v1/accounts/01")
	if err != nil {
		t.Fatalf("get short-id account: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET /v1/accounts/01: got %d, want 400", resp.StatusCode)
	}

	// Transfers reject short debit/credit account IDs too.
	transferBody := `{"id":"000000000000000000000000000000000a","debit_account_id":"01","credit_account_id":"02","amount":"5"}`
	resp, err = http.Post(ts.URL+"/v1/transfers", "application/json", bytes.NewBufferString(transferBody))
	if err != nil {
		t.Fatalf("post transfer with short ids: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /v1/transfers with short account ids: got %d, want 400", resp.StatusCode)
	}

	// Oversized IDs are still rejected.
	resp, err = http.Post(ts.URL+"/v1/accounts", "application/json",
		strings.NewReader(`{"id":"0000000000000000000000000000000111","currency":"USD"}`))
	if err != nil {
		t.Fatalf("post oversized-id account: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /v1/accounts with 17-byte id: got %d, want 400", resp.StatusCode)
	}
}

// C0.12 (gRPC surface): bytesToID must reject IDs that are not exactly 16
// bytes instead of left-padding them into a silently different account.
func TestGRPCRejectsShortIDs(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{
		Id:       []byte{1},
		Currency: "USD",
	})
	grpcStatusEqual(t, err, codes.InvalidArgument, "CreateAccount with 1-byte id")

	trID := mkID(1)
	_, err = client.CreateTransfer(ctx, &ledgerv1.CreateTransferRequest{
		Id:              trID[:],
		DebitAccountId:  []byte{1},
		CreditAccountId: []byte{2},
		Amount:          &ledgerv1.Money{Lo: 5},
	})
	grpcStatusEqual(t, err, codes.InvalidArgument, "CreateTransfer with short account ids")
}
