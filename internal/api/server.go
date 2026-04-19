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

	return &ledgerv1.CreateAccountResponse{
		Account: coreAccountToProto(created),
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

	return &ledgerv1.GetAccountResponse{
		Account: coreAccountToProto(acc),
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
	}

	res, err := h.ledger.CreateTransfer(ctx, tr)
	if err != nil {
		if errors.Is(err, core.ErrNotLeader{}) {
			return nil, status.Errorf(codes.Unavailable, "%v", err)
		}
		if errors.Is(err, core.ErrZeroAmount{}) || errors.Is(err, core.ErrSelfTransfer{}) {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		var notFound core.ErrAccountNotFound
		if errors.As(err, &notFound) {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		var insufficient core.ErrInsufficientFunds
		if errors.As(err, &insufficient) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "transfer execution failed: %v", err)
	}

	return &ledgerv1.CreateTransferResponse{
		Transfer: &ledgerv1.Transfer{
			Id:              res.ID[:],
			DebitAccountId:  res.DebitAccountID[:],
			CreditAccountId: res.CreditAccountID[:],
			Amount:          &ledgerv1.Money{Lo: res.Amount.Lo, Hi: res.Amount.Hi},
			IdempotencyKey:  res.IdempotencyKey[:],
		},
	}, nil
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

func coreAccountToProto(acc core.Account) *ledgerv1.Account {
	bal := core.Balance(acc)
	return &ledgerv1.Account{
		Id:            acc.ID[:],
		Currency:      string(acc.Currency[:]),
		PostedDebits:  &ledgerv1.Money{Lo: acc.PostedDebits.Lo, Hi: acc.PostedDebits.Hi},
		PostedCredits: &ledgerv1.Money{Lo: acc.PostedCredits.Lo, Hi: acc.PostedCredits.Hi},
		Balance:       &ledgerv1.Money{Lo: bal.Lo, Hi: bal.Hi},
	}
}
