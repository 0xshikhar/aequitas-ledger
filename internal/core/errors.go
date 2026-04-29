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

type ErrInvalidRingBufferSize struct{ Size int }

func (e ErrInvalidRingBufferSize) Error() string {
	return fmt.Sprintf("ring buffer size must be a power of two and > 0, got %d", e.Size)
}

// ErrInvariantViolation reports stored account state that violates the
// ledger's non-negative-balance invariant (posted debits exceed posted
// credits). It is returned — never panicked — so the single-writer loop can
// fail the offending item, increment the invariant-violations metric, and
// keep the process alive to be inspected.
type ErrInvariantViolation struct {
	AccountID     [16]byte
	PostedDebits  Uint128
	PostedCredits Uint128
}

func (e ErrInvariantViolation) Error() string {
	return fmt.Sprintf("account invariant violated: %x has debits %s > credits %s",
		e.AccountID, String(e.PostedDebits), String(e.PostedCredits))
}

type ErrPendingNotFound struct{ TransferID [16]byte }

func (e ErrPendingNotFound) Error() string {
	return fmt.Sprintf("pending transfer %x not found (already settled, voided, or expired)", e.TransferID)
}

type ErrInvalidTransferFlags struct{ Flags uint32 }

func (e ErrInvalidTransferFlags) Error() string {
	return fmt.Sprintf("invalid transfer flag combination: 0x%x", e.Flags)
}

type ErrOverTransfer struct {
	TransferID [16]byte
	Pending    Uint128
	Amount     Uint128
}

func (e ErrOverTransfer) Error() string {
	return fmt.Sprintf("post amount %s exceeds pending hold %s for transfer %x",
		String(e.Amount), String(e.Pending), e.TransferID)
}

type ErrTransferNotFound struct{ TransferID [16]byte }

func (e ErrTransferNotFound) Error() string {
	return fmt.Sprintf("transfer not found: %x", e.TransferID)
}

type ErrDuplicateTransferID struct{ TransferID [16]byte }

func (e ErrDuplicateTransferID) Error() string {
	return fmt.Sprintf("duplicate transfer id: %x", e.TransferID)
}

type ErrLedgerMismatch struct {
	TransferLedger uint32
	DebitLedger    uint32
	CreditLedger   uint32
}

func (e ErrLedgerMismatch) Error() string {
	return fmt.Sprintf("ledger mismatch: transfer ledger=%d, debit ledger=%d, credit ledger=%d",
		e.TransferLedger, e.DebitLedger, e.CreditLedger)
}

type ErrLinkedChainFailed struct {
	FailedTransferID [16]byte
	Index            int
	Reason           string
}

func (e ErrLinkedChainFailed) Error() string {
	return fmt.Sprintf("linked transfer failed: transfer %x (index %d) failed: %s",
		e.FailedTransferID, e.Index, e.Reason)
}

type ErrLinkedChainOpen struct {
	TransferID [16]byte
	Index      int
}

func (e ErrLinkedChainOpen) Error() string {
	return fmt.Sprintf("linked transfer chain open: transfer %x (index %d) is the last in batch but has flag_linked set",
		e.TransferID, e.Index)
}

