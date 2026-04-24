package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"aequitas-ledger/internal/api"
	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	"aequitas-ledger/internal/wal"
	ledgerv1 "aequitas-ledger/proto/ledger/v1"
)

// D1.1 engine-level proof: one client batch, positional per-item outcomes,
// exact balances, conservation preserved with mixed valid/invalid items.
func TestEngineCreateTransfersBatchOutcomes(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	l, err := engine.NewLedger(engine.DefaultConfig(), w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	acc1, acc2, acc3 := mkID(1), mkID(2), mkID(3)
	usd := [4]byte{'U', 'S', 'D', 0}
	eur := [4]byte{'E', 'U', 'R', 0}
	for _, acc := range []core.Account{
		{ID: acc1, Currency: usd, PostedCredits: core.Uint128{Lo: 1_000}},
		{ID: acc2, Currency: usd},
		{ID: acc3, Currency: eur}, // different currency: transfers from acc1 must fail
	} {
		if _, err := l.CreateAccount(ctx, acc); err != nil {
			t.Fatalf("create account: %v", err)
		}
	}

	batch := []core.Transfer{
		{ID: mkID(101), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 100}},     // ok
		{ID: mkID(102), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 0}},       // zero amount
		{ID: mkID(103), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10_000}},  // insufficient funds
		{ID: mkID(104), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 50}},      // ok
		{ID: mkID(105), DebitAccountID: acc1, CreditAccountID: mkID(999), Amount: core.Uint128{Lo: 10}}, // unknown credit account
		{ID: mkID(106), DebitAccountID: acc1, CreditAccountID: acc3, Amount: core.Uint128{Lo: 10}},      // currency mismatch
	}

	outcomes, err := l.CreateTransfers(ctx, batch)
	if err != nil {
		t.Fatalf("CreateTransfers: %v", err)
	}
	if len(outcomes) != len(batch) {
		t.Fatalf("got %d outcomes, want %d", len(outcomes), len(batch))
	}
	for _, i := range []int{0, 3} {
		if outcomes[i].Err != nil {
			t.Fatalf("item %d: expected success, got %v", i, outcomes[i].Err)
		}
	}
	for _, i := range []int{1, 2, 4, 5} {
		if outcomes[i].Err == nil {
			t.Fatalf("item %d: expected failure, got success", i)
		}
	}

	// Only items 101 and 104 applied: 150 total debited.
	bal1, err := l.GetBalance(ctx, acc1)
	if err != nil {
		t.Fatalf("balance 1: %v", err)
	}
	if bal1.Lo != 850 {
		t.Fatalf("acc1 balance = %d, want 850", bal1.Lo)
	}
	bal2, _ := l.GetBalance(ctx, acc2)
	if bal2.Lo != 150 {
		t.Fatalf("acc2 balance = %d, want 150", bal2.Lo)
	}

	// Successful items carry server-assigned timestamps.
	for _, i := range []int{0, 3} {
		if outcomes[i].Transfer.Timestamp == 0 {
			t.Fatalf("item %d: committed transfer has no timestamp", i)
		}
	}
}

// D1.1: idempotency within and across batches. A repeated key inside one
// batch conflicts; a replayed batch after commit returns the original
// transfer without re-applying.
func TestEngineBatchIdempotency(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	l, err := engine.NewLedger(engine.DefaultConfig(), w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	acc1, acc2 := mkID(1), mkID(2)
	cur := [4]byte{'U', 'S', 'D', 0}
	if _, err := l.CreateAccount(ctx, core.Account{ID: acc1, Currency: cur, PostedCredits: core.Uint128{Lo: 1_000}}); err != nil {
		t.Fatalf("create acc1: %v", err)
	}
	if _, err := l.CreateAccount(ctx, core.Account{ID: acc2, Currency: cur}); err != nil {
		t.Fatalf("create acc2: %v", err)
	}

	key1 := [32]byte{1}
	batch := []core.Transfer{
		{ID: mkID(1), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10}, IdempotencyKey: key1},
		{ID: mkID(2), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10}, IdempotencyKey: key1},
	}
	outcomes, err := l.CreateTransfers(ctx, batch)
	if err != nil {
		t.Fatalf("CreateTransfers: %v", err)
	}
	if outcomes[0].Err != nil {
		t.Fatalf("item 0 failed: %v", outcomes[0].Err)
	}
	var conflict core.ErrIdempotencyConflict
	if !errorsAs(outcomes[1].Err, &conflict) {
		t.Fatalf("item 1: want idempotency conflict, got %v", outcomes[1].Err)
	}

	// Replay with the same key: returns the original transfer, applies nothing.
	retryOut, err := l.CreateTransfers(ctx, []core.Transfer{
		{ID: mkID(77), DebitAccountID: acc1, CreditAccountID: acc2, Amount: core.Uint128{Lo: 10}, IdempotencyKey: key1},
	})
	if err != nil {
		t.Fatalf("retry CreateTransfers: %v", err)
	}
	if retryOut[0].Err != nil {
		t.Fatalf("retry item failed: %v", retryOut[0].Err)
	}
	if retryOut[0].Transfer.ID != batch[0].ID {
		t.Fatalf("retry returned transfer ID %v, want original %v", retryOut[0].Transfer.ID, batch[0].ID)
	}

	bal1, _ := l.GetBalance(ctx, acc1)
	if bal1.Lo != 990 {
		t.Fatalf("acc1 balance = %d, want 990 (replayed transfer must not re-apply)", bal1.Lo)
	}
}

// D1.1: batches rejected for oversize; follower nodes refuse them.
func TestEngineBatchGuards(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	l, err := engine.NewLedger(engine.DefaultConfig(), w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	_, err = l.CreateTransfers(context.Background(), make([]core.Transfer, engine.MaxTransferBatchSize+1))
	var tooLarge core.ErrBatchTooLarge
	if !errorsAs(err, &tooLarge) {
		t.Fatalf("oversize batch: got %v, want ErrBatchTooLarge", err)
	}

	wf, err := wal.Open(filepath.Join(t.TempDir(), "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open follower wal: %v", err)
	}
	cfg := engine.DefaultConfig()
	cfg.IsFollower = true
	fl, err := engine.NewLedger(cfg, wf)
	if err != nil {
		t.Fatalf("new follower ledger: %v", err)
	}
	defer fl.Close()
	if _, err := fl.CreateTransfers(context.Background(), []core.Transfer{{ID: mkID(1), Amount: core.Uint128{Lo: 1}}}); err == nil {
		t.Fatal("follower accepted a transfer batch")
	}
	if _, err := fl.CreateAccounts(context.Background(), []core.Account{{ID: mkID(1)}}); err == nil {
		t.Fatal("follower accepted an account batch")
	}
}

// D1.1 gRPC proof: batch RPCs return one positional result per item, with
// per-item failure reasons inside a successful RPC.
func TestGRPCBatchRoundTrip(t *testing.T) {
	client, cleanup := setupGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	acc1, acc2 := mkID(1), mkID(2)
	accRes, err := client.CreateAccounts(ctx, &ledgerv1.CreateAccountsRequest{
		Accounts: []*ledgerv1.CreateAccountRequest{
			{Id: acc1[:], Currency: "USD", InitialPostedCredits: &ledgerv1.Money{Lo: 1_000}},
			{Id: acc2[:], Currency: "USD"},
			{Id: acc1[:], Currency: "USD"}, // duplicate — fails per item
		},
	})
	if err != nil {
		t.Fatalf("CreateAccounts batch: %v", err)
	}
	if len(accRes.Results) != 3 {
		t.Fatalf("got %d account results, want 3", len(accRes.Results))
	}
	if !accRes.Results[0].Ok || !accRes.Results[1].Ok {
		t.Fatalf("account items 0/1 should succeed: %q / %q", accRes.Results[0].Error, accRes.Results[1].Error)
	}
	if accRes.Results[2].Ok {
		t.Fatal("account item 2 (duplicate id) should fail")
	}

	tr1, tr2, tr3 := mkID(101), mkID(102), mkID(103)
	trRes, err := client.CreateTransfers(ctx, &ledgerv1.CreateTransfersRequest{
		Transfers: []*ledgerv1.Transfer{
			{Id: tr1[:], DebitAccountId: acc1[:], CreditAccountId: acc2[:], Amount: &ledgerv1.Money{Lo: 100}},
			{Id: tr2[:], DebitAccountId: acc1[:], CreditAccountId: acc2[:], Amount: &ledgerv1.Money{Lo: 999_999}},
			{Id: tr3[:], DebitAccountId: acc1[:], CreditAccountId: acc2[:], Amount: &ledgerv1.Money{Lo: 25}},
		},
	})
	if err != nil {
		t.Fatalf("CreateTransfers batch: %v", err)
	}
	if len(trRes.Results) != 3 {
		t.Fatalf("got %d transfer results, want 3", len(trRes.Results))
	}
	if !trRes.Results[0].Ok || trRes.Results[0].Transfer == nil {
		t.Fatalf("transfer item 0 should succeed: %q", trRes.Results[0].Error)
	}
	if trRes.Results[1].Ok || trRes.Results[1].Error == "" {
		t.Fatal("transfer item 1 should fail with a reason")
	}
	if !trRes.Results[2].Ok {
		t.Fatalf("transfer item 2 should succeed: %q", trRes.Results[2].Error)
	}

	// A single-item batch whose only item is invalid still returns OK with a
	// per-item error, not an RPC error.
	trBad := mkID(201)
	badRes, err := client.CreateTransfers(ctx, &ledgerv1.CreateTransfersRequest{
		Transfers: []*ledgerv1.Transfer{
			{Id: trBad[:], DebitAccountId: acc1[:], CreditAccountId: acc2[:], Amount: &ledgerv1.Money{Lo: 0}},
		},
	})
	if err != nil {
		t.Fatalf("batch with invalid item: %v", err)
	}
	if badRes.Results[0].Ok {
		t.Fatal("zero-amount item should fail per item")
	}
}

// D1.1 REST proof: array endpoints return per-item status inside HTTP 200.
func TestRESTBatchEndpoints(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1<<20)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	l, err := engine.NewLedger(engine.DefaultConfig(), w)
	if err != nil {
		t.Fatalf("new ledger: %v", err)
	}
	defer l.Close()

	ts := httptest.NewServer(api.NewRESTServer(l))
	defer ts.Close()

	accBody := `{"accounts":[
		{"id":"00000000000000000000000000000001","currency":"USD","initial_credits":"1000"},
		{"id":"00000000000000000000000000000002","currency":"USD"},
		{"id":"00000000000000000000000000000001","currency":"USD"}
	]}`
	resp, err := http.Post(ts.URL+"/v1/accounts/batch", "application/json", bytes.NewBufferString(accBody))
	if err != nil {
		t.Fatalf("post accounts batch: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("accounts batch: got %d, want 200 (body %s)", resp.StatusCode, body)
	}
	var accResults struct {
		Results []struct {
			Index int    `json:"index"`
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &accResults); err != nil {
		t.Fatalf("decode accounts batch response: %v", err)
	}
	if len(accResults.Results) != 3 || !accResults.Results[0].OK || !accResults.Results[1].OK || accResults.Results[2].OK {
		t.Fatalf("unexpected account results: %s", body)
	}

	trBody := `{"transfers":[
		{"id":"0000000000000000000000000000000a","debit_account_id":"00000000000000000000000000000001","credit_account_id":"00000000000000000000000000000002","amount":"100","idempotency_key":"k1"},
		{"id":"0000000000000000000000000000000b","debit_account_id":"00000000000000000000000000000001","credit_account_id":"00000000000000000000000000000002","amount":"999999"},
		{"id":"0000000000000000000000000000000c","debit_account_id":"00000000000000000000000000000001","credit_account_id":"00000000000000000000000000000002","amount":"25"}
	]}`
	resp, err = http.Post(ts.URL+"/v1/transfers/batch", "application/json", bytes.NewBufferString(trBody))
	if err != nil {
		t.Fatalf("post transfers batch: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("transfers batch: got %d, want 200 (body %s)", resp.StatusCode, body)
	}
	var trResults struct {
		Results []struct {
			Index    int    `json:"index"`
			OK       bool   `json:"ok"`
			Error    string `json:"error"`
			Transfer *struct {
				ID string `json:"id"`
			} `json:"transfer"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &trResults); err != nil {
		t.Fatalf("decode transfers batch response: %v", err)
	}
	if len(trResults.Results) != 3 {
		t.Fatalf("want 3 transfer results, got %d", len(trResults.Results))
	}
	if !trResults.Results[0].OK || trResults.Results[0].Transfer == nil {
		t.Fatalf("transfer item 0 should succeed: %+v", trResults.Results[0])
	}
	if trResults.Results[1].OK || trResults.Results[1].Error == "" {
		t.Fatal("transfer item 1 should fail with a reason")
	}
	if !trResults.Results[2].OK {
		t.Fatalf("transfer item 2 should succeed: %+v", trResults.Results[2])
	}

	resp, err = http.Post(ts.URL+"/v1/transfers/batch", "application/json", bytes.NewBufferString(`{"transfers":[]}`))
	if err != nil {
		t.Fatalf("post empty batch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty batch: got %d, want 400", resp.StatusCode)
	}
}

// errorsAs is a small wrapper so tests read tersely.
func errorsAs(err error, target any) bool {
	if err == nil {
		return false
	}
	return errors.As(err, target)
}
