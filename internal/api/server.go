package api

import (
	"context"
	"crypto/sha256"
	"errors"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/engine"
	ledgerv1 "aequitas-ledger/proto/ledger/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AccountsHandler struct {
	ledgerv1.UnimplementedLedgerServiceServer
	ledger *engine.Ledger
}

func NewAccountsHandler(ledger *engine.Ledger) *AccountsHandler {
	return &AccountsHandler{ledger: ledger}
}

func (h *AccountsHandler) CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.CreateAccountResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}

	accID, err := bytesToID(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid account id: %v", err)
	}

	var currency [4]byte
	copy(currency[:], []byte(req.Currency))

	var postedCredits core.Uint128
	if req.InitialPostedCredits != nil {
		postedCredits = core.Uint128{Lo: req.InitialPostedCredits.Lo, Hi: req.InitialPostedCredits.Hi}
	}

	acc := core.Account{
		ID:            accID,
		Currency:      currency,
		PostedCredits: postedCredits,
	}

	created, err := h.ledger.CreateAccount(ctx, acc)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			return nil, status.Errorf(codes.Unavailable, "%v", err)
		}
		var dup core.ErrDuplicateAccountID
		if errors.As(err, &dup) {
			return nil, status.Errorf(codes.AlreadyExists, "account already exists: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to create account: %v", err)
	}

	protoAcc, err := coreAccountToProto(created)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err.Error())
	}
	return &ledgerv1.CreateAccountResponse{
		Account: protoAcc,
	}, nil
}

func (h *AccountsHandler) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.GetAccountResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}

	accID, err := bytesToID(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid account id: %v", err)
	}

	acc, err := h.ledger.GetAccount(ctx, accID)
	if err != nil {
		var notFound core.ErrAccountNotFound
		if errors.As(err, &notFound) {
			return nil, status.Errorf(codes.NotFound, "account not found: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to get account: %v", err)
	}

	protoAcc, err := coreAccountToProto(acc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err.Error())
	}
	return &ledgerv1.GetAccountResponse{
		Account: protoAcc,
	}, nil
}

func (h *AccountsHandler) CreateTransfer(ctx context.Context, req *ledgerv1.CreateTransferRequest) (*ledgerv1.CreateTransferResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}

	trID, err := bytesToID(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid transfer id: %v", err)
	}

	debitID, err := bytesToID(req.DebitAccountId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid debit account id: %v", err)
	}

	creditID, err := bytesToID(req.CreditAccountId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid credit account id: %v", err)
	}

	if req.Amount == nil {
		return nil, status.Error(codes.InvalidArgument, "missing transfer amount")
	}
	amount := core.Uint128{Lo: req.Amount.Lo, Hi: req.Amount.Hi}

	var key [32]byte
	if len(req.IdempotencyKey) == 32 {
		copy(key[:], req.IdempotencyKey)
	} else if len(req.IdempotencyKey) > 0 {
		key = sha256.Sum256(req.IdempotencyKey)
	} else if ctxKey, ok := IdempotencyKeyFromContext(ctx); ok {
		key = ctxKey
	}

	tr := core.Transfer{
		ID:              trID,
		DebitAccountID:  debitID,
		CreditAccountID: creditID,
		Amount:          amount,
		IdempotencyKey:  key,
		Flags:           req.Flags,
		Timeout:         req.Timeout,
	}

	res, err := h.ledger.CreateTransfer(ctx, tr)
	if err != nil {
		code, msg := transferErrorStatus(err)
		return nil, status.Errorf(code, "%s", msg)
	}

	return &ledgerv1.CreateTransferResponse{
		Transfer: coreTransferToProto(res),
	}, nil
}

// CreateTransfers implements the batched protocol (D1.1): one RPC, up to 8192
// transfers, one positional result per item. The batch itself is accepted at
// the protocol level — per-item failures are reported in the results array,
// never as an RPC error.
func (h *AccountsHandler) CreateTransfers(ctx context.Context, req *ledgerv1.CreateTransfersRequest) (*ledgerv1.CreateTransfersResponse, error) {
	if req == nil || len(req.Transfers) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty transfer batch")
	}

	batch := make([]core.Transfer, len(req.Transfers))
	for i, pt := range req.Transfers {
		if pt == nil {
			return nil, status.Errorf(codes.InvalidArgument, "transfer %d is nil", i)
		}
		trID, err := bytesToID(pt.Id)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "transfer %d: invalid id: %v", i, err)
		}
		debitID, err := bytesToID(pt.DebitAccountId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "transfer %d: invalid debit account id: %v", i, err)
		}
		creditID, err := bytesToID(pt.CreditAccountId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "transfer %d: invalid credit account id: %v", i, err)
		}
		if pt.Amount == nil {
			return nil, status.Errorf(codes.InvalidArgument, "transfer %d: missing amount", i)
		}

		var key [32]byte
		switch {
		case len(pt.IdempotencyKey) == 32:
			copy(key[:], pt.IdempotencyKey)
		case len(pt.IdempotencyKey) > 0:
			key = sha256.Sum256(pt.IdempotencyKey)
		default:
			if ctxKey, ok := IdempotencyKeyFromContext(ctx); ok {
				key = ctxKey
			}
		}

		batch[i] = core.Transfer{
			ID:              trID,
			DebitAccountID:  debitID,
			CreditAccountID: creditID,
			Amount:          core.Uint128{Lo: pt.Amount.Lo, Hi: pt.Amount.Hi},
			IdempotencyKey:  key,
			Flags:           pt.Flags,
			Timeout:         pt.Timeout,
		}
	}

	outcomes, err := h.ledger.CreateTransfers(ctx, batch)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		var tooLarge core.ErrBatchTooLarge
		if errors.As(err, &tooLarge) {
			return nil, status.Errorf(codes.InvalidArgument, "%s", err.Error())
		}
		return nil, status.Errorf(codes.Internal, "batch execution failed: %v", err)
	}

	results := make([]*ledgerv1.TransferResult, len(outcomes))
	for i := range outcomes {
		if outcomes[i].Err != nil {
			_, msg := transferErrorStatus(outcomes[i].Err)
			results[i] = &ledgerv1.TransferResult{Ok: false, Error: msg}
			continue
		}
		results[i] = &ledgerv1.TransferResult{Ok: true, Transfer: coreTransferToProto(outcomes[i].Transfer)}
	}
	return &ledgerv1.CreateTransfersResponse{Results: results}, nil
}

// CreateAccounts is the batched account-creation RPC: one positional result
// per requested account.
func (h *AccountsHandler) CreateAccounts(ctx context.Context, req *ledgerv1.CreateAccountsRequest) (*ledgerv1.CreateAccountsResponse, error) {
	if req == nil || len(req.Accounts) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty account batch")
	}

	batch := make([]core.Account, len(req.Accounts))
	for i, pa := range req.Accounts {
		if pa == nil {
			return nil, status.Errorf(codes.InvalidArgument, "account %d is nil", i)
		}
		accID, err := bytesToID(pa.Id)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "account %d: invalid id: %v", i, err)
		}
		var currency [4]byte
		copy(currency[:], []byte(pa.Currency))

		var postedCredits core.Uint128
		if pa.InitialPostedCredits != nil {
			postedCredits = core.Uint128{Lo: pa.InitialPostedCredits.Lo, Hi: pa.InitialPostedCredits.Hi}
		}
		batch[i] = core.Account{ID: accID, Currency: currency, PostedCredits: postedCredits}
	}

	outcomes, err := h.ledger.CreateAccounts(ctx, batch)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		var tooLarge core.ErrBatchTooLarge
		if errors.As(err, &tooLarge) {
			return nil, status.Errorf(codes.InvalidArgument, "%s", err.Error())
		}
		return nil, status.Errorf(codes.Internal, "account batch failed: %v", err)
	}

	results := make([]*ledgerv1.AccountResult, len(outcomes))
	for i := range outcomes {
		if outcomes[i].Err != nil {
			results[i] = &ledgerv1.AccountResult{Ok: false, Error: outcomes[i].Err.Error()}
			continue
		}
		protoAcc, perr := coreAccountToProto(outcomes[i].Account)
		if perr != nil {
			results[i] = &ledgerv1.AccountResult{Ok: false, Error: perr.Error()}
			continue
		}
		results[i] = &ledgerv1.AccountResult{Ok: true, Account: protoAcc}
	}
	return &ledgerv1.CreateAccountsResponse{Results: results}, nil
}

func (h *AccountsHandler) GetTransfer(ctx context.Context, req *ledgerv1.GetTransferRequest) (*ledgerv1.GetTransferResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	trID, err := bytesToID(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid transfer id: %v", err)
	}
	tr, err := h.ledger.GetTransfer(ctx, trID)
	if err != nil {
		var notFound core.ErrTransferNotFound
		if errors.As(err, &notFound) {
			return nil, status.Errorf(codes.NotFound, "transfer not found: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to get transfer: %v", err)
	}
	return &ledgerv1.GetTransferResponse{Transfer: coreTransferToProto(tr)}, nil
}

func (h *AccountsHandler) GetAccountTransfers(ctx context.Context, req *ledgerv1.GetAccountTransfersRequest) (*ledgerv1.GetAccountTransfersResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "nil request")
	}
	accID, err := bytesToID(req.AccountId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid account id: %v", err)
	}
	var afterID [16]byte
	if len(req.AfterId) > 0 {
		afterID, err = bytesToID(req.AfterId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid after_id: %v", err)
		}
	}
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 50
	}
	transfers, err := h.ledger.GetAccountTransfers(ctx, accID, afterID, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get account transfers: %v", err)
	}
	protoTransfers := make([]*ledgerv1.Transfer, len(transfers))
	for i, tr := range transfers {
		protoTransfers[i] = coreTransferToProto(tr)
	}
	return &ledgerv1.GetAccountTransfersResponse{Transfers: protoTransfers}, nil
}

func coreTransferToProto(t core.Transfer) *ledgerv1.Transfer {
	return &ledgerv1.Transfer{
		Id:              t.ID[:],
		DebitAccountId:  t.DebitAccountID[:],
		CreditAccountId: t.CreditAccountID[:],
		Amount:          &ledgerv1.Money{Lo: t.Amount.Lo, Hi: t.Amount.Hi},
		IdempotencyKey:  t.IdempotencyKey[:],
		CreatedAt:       t.Timestamp,
		Flags:           t.Flags,
		Timeout:         t.Timeout,
	}
}

// transferErrorStatus maps a transfer failure to its gRPC code. Used by both
// the unary handler and the per-item results of the batch handler so the two
// surfaces agree (C0.11's lesson).
func transferErrorStatus(err error) (codes.Code, string) {
	if errors.Is(err, core.ErrNotLeader{}) {
		return codes.Unavailable, err.Error()
	}
	if errors.Is(err, core.ErrZeroAmount{}) || errors.Is(err, core.ErrSelfTransfer{}) {
		return codes.InvalidArgument, err.Error()
	}
	var invalidFlags core.ErrInvalidTransferFlags
	if errors.As(err, &invalidFlags) {
		return codes.InvalidArgument, err.Error()
	}
	var notFound core.ErrAccountNotFound
	if errors.As(err, &notFound) {
		return codes.NotFound, err.Error()
	}
	var trNotFound core.ErrTransferNotFound
	if errors.As(err, &trNotFound) {
		return codes.NotFound, err.Error()
	}
	var pendingNotFound core.ErrPendingNotFound
	if errors.As(err, &pendingNotFound) {
		return codes.NotFound, err.Error()
	}
	var insufficient core.ErrInsufficientFunds
	if errors.As(err, &insufficient) {
		return codes.FailedPrecondition, err.Error()
	}
	var overTransfer core.ErrOverTransfer
	if errors.As(err, &overTransfer) {
		return codes.FailedPrecondition, err.Error()
	}
	return codes.Internal, err.Error()
}

func bytesToID(b []byte) ([16]byte, error) {
	var id [16]byte
	if len(b) == 0 {
		return id, errors.New("empty id")
	}
	if len(b) != 16 {
		return id, errors.New("id must be exactly 16 bytes")
	}
	copy(id[:], b)
	return id, nil
}

func coreAccountToProto(acc core.Account) (*ledgerv1.Account, error) {
	bal, err := core.Balance(acc)
	if err != nil {
		return nil, err
	}
	avail, err := core.AvailableBalance(acc)
	if err != nil {
		return nil, err
	}
	return &ledgerv1.Account{
		Id:               acc.ID[:],
		Currency:         string(acc.Currency[:]),
		PostedDebits:     &ledgerv1.Money{Lo: acc.PostedDebits.Lo, Hi: acc.PostedDebits.Hi},
		PostedCredits:    &ledgerv1.Money{Lo: acc.PostedCredits.Lo, Hi: acc.PostedCredits.Hi},
		PendingDebits:    &ledgerv1.Money{Lo: acc.PendingDebits.Lo, Hi: acc.PendingDebits.Hi},
		PendingCredits:   &ledgerv1.Money{Lo: acc.PendingCredits.Lo, Hi: acc.PendingCredits.Hi},
		Balance:          &ledgerv1.Money{Lo: bal.Lo, Hi: bal.Hi},
		AvailableBalance: &ledgerv1.Money{Lo: avail.Lo, Hi: avail.Hi},
	}, nil
}
