package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
)

func TestRESTAPIWorkflow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aequitas-rest-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

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

	// 1. Health check
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected health check 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 2. Create Account 1 (Debit account with initial credits)
	acc1Body := `{"id": "00000000000000000000000000000001", "currency": "USD", "initial_credits": "1000"}`
	resp, err = http.Post(ts.URL+"/v1/accounts", "application/json", bytes.NewBufferString(acc1Body))
	if err != nil {
		t.Fatalf("create account 1 failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201 Created for account 1, got %d: %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()

	// 3. Create Account 2 (Credit account)
	acc2Body := `{"id": "00000000000000000000000000000002", "currency": "USD", "initial_credits": "0"}`
	resp, err = http.Post(ts.URL+"/v1/accounts", "application/json", bytes.NewBufferString(acc2Body))
	if err != nil {
		t.Fatalf("create account 2 failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for account 2, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 4. Create Transfer via REST with Idempotency Header
	trReqBody := `{"id": "000000000000000000000000000000a1", "debit_account_id": "00000000000000000000000000000001", "credit_account_id": "00000000000000000000000000000002", "amount": "250"}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/transfers", bytes.NewBufferString(trReqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "rest-transfer-key-100")

	client := &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("create transfer failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201 Created for transfer, got %d: %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()

	// 5. Verify Account 1 Balance (1000 - 250 = 750)
	resp, err = http.Get(ts.URL + "/v1/accounts/00000000000000000000000000000001")
	if err != nil {
		t.Fatalf("get account 1 failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for account 1 get, got %d", resp.StatusCode)
	}
	var acc1Data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&acc1Data)
	resp.Body.Close()

	if fmt.Sprintf("%v", acc1Data["balance"]) != "750" {
		t.Errorf("expected account 1 balance '750', got '%v'", acc1Data["balance"])
	}

	// 6. Verify Account 2 Balance (0 + 250 = 250)
	resp, err = http.Get(ts.URL + "/v1/accounts/00000000000000000000000000000002")
	if err != nil {
		t.Fatalf("get account 2 failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for account 2 get, got %d", resp.StatusCode)
	}
	var acc2Data map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&acc2Data)
	resp.Body.Close()

	if fmt.Sprintf("%v", acc2Data["balance"]) != "250" {
		t.Errorf("expected account 2 balance '250', got '%v'", acc2Data["balance"])
	}

	// 7. Query transfer by ID via REST
	resp, err = http.Get(ts.URL + "/v1/transfers/000000000000000000000000000000a1")
	if err != nil {
		t.Fatalf("get transfer failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for transfer get, got %d", resp.StatusCode)
	}
	var trData map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&trData)
	resp.Body.Close()
	if fmt.Sprintf("%v", trData["amount"]) != "250" {
		t.Fatalf("expected transfer amount '250', got '%v'", trData["amount"])
	}

	// 8. Query account transfers via REST
	resp, err = http.Get(ts.URL + "/v1/accounts/00000000000000000000000000000001/transfers")
	if err != nil {
		t.Fatalf("get account transfers failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for account transfers, got %d", resp.StatusCode)
	}
	var trList []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&trList)
	resp.Body.Close()
	if len(trList) != 1 {
		t.Fatalf("expected 1 transfer in list, got %d", len(trList))
	}

	// 9. Two-Phase Hold via REST
	holdReq := `{"id": "000000000000000000000000000000b1", "debit_account_id": "00000000000000000000000000000001", "credit_account_id": "00000000000000000000000000000002", "amount": "200", "flags": 1}`
	resp, err = http.Post(ts.URL+"/v1/transfers", "application/json", bytes.NewBufferString(holdReq))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("create hold failed, status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(ts.URL + "/v1/accounts/00000000000000000000000000000001")
	json.NewDecoder(resp.Body).Decode(&acc1Data)
	resp.Body.Close()
	if fmt.Sprintf("%v", acc1Data["available_balance"]) != "550" {
		t.Fatalf("expected available_balance 550 after hold, got %v", acc1Data["available_balance"])
	}

	// Void hold via REST
	voidReq := `{"id": "000000000000000000000000000000b1", "debit_account_id": "00000000000000000000000000000001", "credit_account_id": "00000000000000000000000000000002", "flags": 4}`
	resp, err = http.Post(ts.URL+"/v1/transfers", "application/json", bytes.NewBufferString(voidReq))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("void hold failed, status: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(ts.URL + "/v1/accounts/00000000000000000000000000000001")
	json.NewDecoder(resp.Body).Decode(&acc1Data)
	resp.Body.Close()
	if fmt.Sprintf("%v", acc1Data["available_balance"]) != "750" {
		t.Fatalf("expected available_balance 750 after void, got %v", acc1Data["available_balance"])
	}
}
