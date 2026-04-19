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
	ledger *engine.Ledger
	mux    *http.ServeMux
}

func NewRESTServer(ledger *engine.Ledger) *RESTServer {
	s := &RESTServer{
		ledger: ledger,
		mux:    http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *RESTServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *RESTServer) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/v1/accounts", s.handleAccounts)
	s.mux.HandleFunc("/v1/accounts/", s.handleGetAccount)
	s.mux.HandleFunc("/v1/transfers", s.handleCreateTransfer)
}

func (s *RESTServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
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

	writeJSON(w, http.StatusCreated, formatAccountResp(created))
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

	writeJSON(w, http.StatusOK, formatAccountResp(acc))
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
	if len(idempHeader) > 0 {
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

func formatAccountResp(acc core.Account) accountResp {
	bal := core.Balance(acc)
	return accountResp{
		ID:            fmt.Sprintf("%x", acc.ID[:]),
		Currency:      strings.TrimRight(string(acc.Currency[:]), "\x00"),
		PostedDebits:  core.String(acc.PostedDebits),
		PostedCredits: core.String(acc.PostedCredits),
		Balance:       core.String(bal),
	}
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
