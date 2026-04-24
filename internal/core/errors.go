package core

import "fmt"

type ErrInsufficientFunds struct {
	AccountID [16]byte
	Balance   Uint128
	Amount    Uint128
}

func (e ErrInsufficientFunds) Error() string {
	return fmt.Sprintf("insufficient funds: account=%x balance=%s amount=%s", e.AccountID, String(e.Balance), String(e.Amount))
}

type ErrAccountNotFound struct{ AccountID [16]byte }

func (e ErrAccountNotFound) Error() string {
	return fmt.Sprintf("account not found: %x", e.AccountID)
}

type ErrDuplicateAccountID struct{ AccountID [16]byte }

func (e ErrDuplicateAccountID) Error() string {
	return fmt.Sprintf("duplicate account id: %x", e.AccountID)
}

type ErrAccountFrozen struct{ AccountID [16]byte }

func (e ErrAccountFrozen) Error() string {
	return fmt.Sprintf("account frozen: %x", e.AccountID)
}

type ErrAccountClosed struct{ AccountID [16]byte }

func (e ErrAccountClosed) Error() string {
	return fmt.Sprintf("account closed: %x", e.AccountID)
}

type ErrSelfTransfer struct{ AccountID [16]byte }

func (e ErrSelfTransfer) Error() string {
	return fmt.Sprintf("self transfer is not allowed: %x", e.AccountID)
}

type ErrZeroAmount struct{}

func (ErrZeroAmount) Error() string { return "transfer amount must be greater than zero" }

type ErrCurrencyMismatch struct {
	DebitCurrency  [4]byte
	CreditCurrency [4]byte
}

func (e ErrCurrencyMismatch) Error() string {
	return fmt.Sprintf("currency mismatch: debit=%q credit=%q", e.DebitCurrency, e.CreditCurrency)
}

type ErrBalanceOverflow struct{ AccountID [16]byte }

func (e ErrBalanceOverflow) Error() string {
	return fmt.Sprintf("balance overflow: %x", e.AccountID)
}

type ErrIdempotencyConflict struct{ Key [32]byte }

func (e ErrIdempotencyConflict) Error() string {
	return fmt.Sprintf("idempotency conflict: %x", e.Key)
}

type ErrBufferFull struct{}

func (ErrBufferFull) Error() string { return "submission buffer is full" }

type ErrDuplicateTransferID struct{ ID [16]byte }

func (e ErrDuplicateTransferID) Error() string {
	return fmt.Sprintf("duplicate transfer id: %x", e.ID)
}

type ErrNotLeader struct{}

func (ErrNotLeader) Error() string { return "node is running as a read-only follower (not leader)" }

type ErrLedgerClosed struct{}

func (ErrLedgerClosed) Error() string { return "ledger is shutting down; batch was not applied" }

type ErrBatchTooLarge struct {
	Got int
	Max int
}

func (e ErrBatchTooLarge) Error() string {
	return fmt.Sprintf("batch too large: %d items exceeds maximum of %d", e.Got, e.Max)
}
