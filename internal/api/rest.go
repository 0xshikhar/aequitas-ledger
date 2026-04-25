package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
)

type RESTServer struct {
	ledger    *engine.Ledger
	mux       *http.ServeMux
	readiness func() bool // nil → ledger.Ready()
}

// RESTOption customizes the REST server.
type RESTOption func(*RESTServer)

// WithReadiness overrides the /readyz signal (e.g. a follower adds its
// replication catch-up state to the ledger's own readiness).
func WithReadiness(fn func() bool) RESTOption {
	return func(s *RESTServer) { s.readiness = fn }
}

func NewRESTServer(ledger *engine.Ledger, opts ...RESTOption) *RESTServer {
	s := &RESTServer{
		ledger: ledger,
		mux:    http.NewServeMux(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.registerRoutes()
	return s
}

func (s *RESTServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *RESTServer) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/readyz", s.handleReadyz)
	s.mux.HandleFunc("/v1/accounts", s.handleAccounts)
	s.mux.HandleFunc("/v1/accounts/batch", s.handleBatchAccounts)
	s.mux.HandleFunc("/v1/accounts/", s.handleGetAccount)
	s.mux.HandleFunc("/v1/transfers", s.handleCreateTransfer)
	s.mux.HandleFunc("/v1/transfers/batch", s.handleBatchTransfers)
}

func (s *RESTServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

// handleReadyz reports whether the node should receive traffic: recovery is
// complete and (with a checkpointer configured) the first snapshot cycle
// succeeded. Liveness (healthz) means the process is up; readiness means it
// is safe to route to.
func (s *RESTServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ready := s.ledger.Ready()
	if s.readiness != nil {
		ready = s.readiness()
	}
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "NOT_READY"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "READY"})
}

type createAccountReq struct {
	ID             string `json:"id"`
	Currency       string `json:"currency"`
	InitialCredits string `json:"initial_credits"`
}

type accountResp struct {
	ID            string `json:"id"`
	Currency      string `json:"currency"`
	PostedDebits  string `json:"posted_debits"`
	PostedCredits string `json:"posted_credits"`
	Balance       string `json:"balance"`
}

func (s *RESTServer) handleAccounts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req createAccountReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	accID, err := parseHexID(req.ID)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid account id: %v", err))
		return
	}

	var curr [4]byte
	copy(curr[:], []byte(req.Currency))

	var initialCredits core.Uint128
	if req.InitialCredits != "" {
		ic, err := core.FromString(req.InitialCredits)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid initial_credits: %v", err))
			return
		}
		initialCredits = ic
	}

	acc := core.Account{
		ID:            accID,
		Currency:      curr,
		PostedCredits: initialCredits,
	}

	created, err := s.ledger.CreateAccount(r.Context(), acc)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		var dup core.ErrDuplicateAccountID
		if errors.As(err, &dup) {
			writeError(w, http.StatusConflict, "account already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp, err := formatAccountResp(created)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *RESTServer) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/v1/accounts/")
	if idStr == "" {
		writeError(w, http.StatusBadRequest, "missing account id")
		return
	}

	accID, err := parseHexID(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid account id")
		return
	}

	acc, err := s.ledger.GetAccount(r.Context(), accID)
	if err != nil {
		var notFound core.ErrAccountNotFound
		if errors.As(err, &notFound) {
			writeError(w, http.StatusNotFound, "account not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp, err := formatAccountResp(acc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type createTransferReq struct {
	ID              string `json:"id"`
	DebitAccountID  string `json:"debit_account_id"`
	CreditAccountID string `json:"credit_account_id"`
	Amount          string `json:"amount"`
	IdempotencyKey  string `json:"idempotency_key"`
}

type transferResp struct {
	ID              string `json:"id"`
	DebitAccountID  string `json:"debit_account_id"`
	CreditAccountID string `json:"credit_account_id"`
	Amount          string `json:"amount"`
	IdempotencyKey  string `json:"idempotency_key"`
}

func (s *RESTServer) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req createTransferReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	trID, err := parseHexID(req.ID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid transfer id")
		return
	}

	debitID, err := parseHexID(req.DebitAccountID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid debit account id")
		return
	}

	creditID, err := parseHexID(req.CreditAccountID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid credit account id")
		return
	}

	amount, err := core.FromString(req.Amount)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}

	idempHeader := r.Header.Get("Idempotency-Key")
	if idempHeader == "" {
		idempHeader = req.IdempotencyKey
	}

	var key [32]byte
	if idempHeader != "" {
		// Absent key stays zero: the engine treats a zero key as "no
		// idempotency check". Hashing the empty string would collapse all
		// keyless transfers onto one fixed key.
		key = sha256.Sum256([]byte(idempHeader))
	}

	tr := core.Transfer{
		ID:              trID,
		DebitAccountID:  debitID,
		CreditAccountID: creditID,
		Amount:          amount,
		IdempotencyKey:  key,
	}

	res, err := s.ledger.CreateTransfer(r.Context(), tr)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if errors.Is(err, core.ErrZeroAmount{}) || errors.Is(err, core.ErrSelfTransfer{}) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		var notFound core.ErrAccountNotFound
		if errors.As(err, &notFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		var insufficient core.ErrInsufficientFunds
		if errors.As(err, &insufficient) {
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, transferResp{
		ID:              fmt.Sprintf("%x", res.ID[:]),
		DebitAccountID:  fmt.Sprintf("%x", res.DebitAccountID[:]),
		CreditAccountID: fmt.Sprintf("%x", res.CreditAccountID[:]),
		Amount:          core.String(res.Amount),
		IdempotencyKey:  fmt.Sprintf("%x", res.IdempotencyKey[:]),
	})
}

// --- Batch endpoints (D1.1) ----------------------------------------------
//
// The batch endpoints accept arrays and return one positional result per
// item. The batch itself is accepted at the protocol level: a 200 response
// carries per-item ok/error even when some items failed, mirroring
// TigerBeetle's batch semantics. Item-shaped parse failures of the request
// body itself (bad JSON, unknown fields structure) are 400s before the
// engine sees anything.

type batchTransfersReq struct {
	Transfers []createTransferReq `json:"transfers"`
}

type transferResultJSON struct {
	Index    int           `json:"index"`
	OK       bool          `json:"ok"`
	Transfer *transferResp `json:"transfer,omitempty"`
	Error    string        `json:"error,omitempty"`
}

func (s *RESTServer) handleBatchTransfers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req batchTransfersReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if len(req.Transfers) == 0 {
		writeError(w, http.StatusBadRequest, "empty transfer batch")
		return
	}
	if len(req.Transfers) > engine.MaxTransferBatchSize {
		writeError(w, http.StatusBadRequest, fmt.Sprintf(
			"batch too large: %d items exceeds maximum of %d", len(req.Transfers), engine.MaxTransferBatchSize))
		return
	}

	batch := make([]core.Transfer, len(req.Transfers))
	for i := range req.Transfers {
		tr, err := s.parseTransferReq(&req.Transfers[i])
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("transfer %d: %v", i, err))
			return
		}
		batch[i] = tr
	}

	outcomes, err := s.ledger.CreateTransfers(r.Context(), batch)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	results := make([]transferResultJSON, len(outcomes))
	for i := range outcomes {
		results[i].Index = i
		if outcomes[i].Err != nil {
			results[i].Error = outcomes[i].Err.Error()
			continue
		}
		results[i].OK = true
		resp := transferResultToResp(outcomes[i].Transfer)
		results[i].Transfer = &resp
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// parseTransferReq converts one REST transfer item to a core.Transfer,
// applying the same strict parsing as the single-transfer endpoint.
func (s *RESTServer) parseTransferReq(req *createTransferReq) (core.Transfer, error) {
	trID, err := parseHexID(req.ID)
	if err != nil {
		return core.Transfer{}, fmt.Errorf("invalid transfer id: %v", err)
	}
	debitID, err := parseHexID(req.DebitAccountID)
	if err != nil {
		return core.Transfer{}, fmt.Errorf("invalid debit account id: %v", err)
	}
	creditID, err := parseHexID(req.CreditAccountID)
	if err != nil {
		return core.Transfer{}, fmt.Errorf("invalid credit account id: %v", err)
	}
	amount, err := core.FromString(req.Amount)
	if err != nil {
		return core.Transfer{}, fmt.Errorf("invalid amount: %v", err)
	}

	var key [32]byte
	if req.IdempotencyKey != "" {
		key = sha256.Sum256([]byte(req.IdempotencyKey))
	}
	return core.Transfer{
		ID:              trID,
		DebitAccountID:  debitID,
		CreditAccountID: creditID,
		Amount:          amount,
		IdempotencyKey:  key,
	}, nil
}

func transferResultToResp(t core.Transfer) transferResp {
	return transferResp{
		ID:              fmt.Sprintf("%x", t.ID[:]),
		DebitAccountID:  fmt.Sprintf("%x", t.DebitAccountID[:]),
		CreditAccountID: fmt.Sprintf("%x", t.CreditAccountID[:]),
		Amount:          core.String(t.Amount),
		IdempotencyKey:  fmt.Sprintf("%x", t.IdempotencyKey[:]),
	}
}

type batchAccountsReq struct {
	Accounts []createAccountReq `json:"accounts"`
}

type accountResultJSON struct {
	Index   int          `json:"index"`
	OK      bool         `json:"ok"`
	Account *accountResp `json:"account,omitempty"`
	Error   string       `json:"error,omitempty"`
}

func (s *RESTServer) handleBatchAccounts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req batchAccountsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if len(req.Accounts) == 0 {
		writeError(w, http.StatusBadRequest, "empty account batch")
		return
	}
	if len(req.Accounts) > engine.MaxAccountBatchSize {
		writeError(w, http.StatusBadRequest, fmt.Sprintf(
			"batch too large: %d items exceeds maximum of %d", len(req.Accounts), engine.MaxAccountBatchSize))
		return
	}

	batch := make([]core.Account, len(req.Accounts))
	for i := range req.Accounts {
		item := &req.Accounts[i]
		accID, err := parseHexID(item.ID)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("account %d: invalid id: %v", i, err))
			return
		}
		var curr [4]byte
		copy(curr[:], []byte(item.Currency))

		var initialCredits core.Uint128
		if item.InitialCredits != "" {
			ic, err := core.FromString(item.InitialCredits)
			if err != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("account %d: invalid initial_credits: %v", i, err))
				return
			}
			initialCredits = ic
		}
		batch[i] = core.Account{ID: accID, Currency: curr, PostedCredits: initialCredits}
	}

	outcomes, err := s.ledger.CreateAccounts(r.Context(), batch)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	results := make([]accountResultJSON, len(outcomes))
	for i := range outcomes {
		results[i].Index = i
		if outcomes[i].Err != nil {
			results[i].Error = outcomes[i].Err.Error()
			continue
		}
		results[i].OK = true
		resp, ferr := formatAccountResp(outcomes[i].Account)
		if ferr != nil {
			results[i].Error = ferr.Error()
			continue
		}
		results[i].Account = &resp
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func parseHexID(s string) ([16]byte, error) {
	var id [16]byte
	s = strings.TrimPrefix(s, "0x")
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(b) != 16 {
		return id, errors.New("id must be exactly 16 bytes (32 hex chars)")
	}
	copy(id[:], b)
	return id, nil
}

func formatAccountResp(acc core.Account) (accountResp, error) {
	bal, err := core.Balance(acc)
	if err != nil {
		return accountResp{}, err
	}
	return accountResp{
		ID:            fmt.Sprintf("%x", acc.ID[:]),
		Currency:      strings.TrimRight(string(acc.Currency[:]), "\x00"),
		PostedDebits:  core.String(acc.PostedDebits),
		PostedCredits: core.String(acc.PostedCredits),
		Balance:       core.String(bal),
	}, nil
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
