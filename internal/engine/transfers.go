package engine

import (
	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/observability"
)

func ValidateBatch(events []TransferEvent, accounts *AccountManager) []error {
	outcomes := make([]error, len(events))
	for i := range events {
		outcomes[i] = accounts.ValidateTransfer(events[i].Transfer)
	}
	return outcomes
}

func ApplyBatch(events []TransferEvent, outcomes []error, accounts *AccountManager) {
	for i := range events {
		if outcomes[i] != nil {
			continue
		}
		t := events[i].Transfer

		debitAcc, err := accounts.Get(t.DebitAccountID)
		if err != nil {
			outcomes[i] = err
			continue
		}
		bal, err := core.Balance(*debitAcc)
		if err != nil {
			observability.InvariantViolations.Inc()
			outcomes[i] = err
			continue
		}
		if core.Cmp(bal, t.Amount) < 0 {
			outcomes[i] = core.ErrInsufficientFunds{AccountID: t.DebitAccountID, Balance: bal, Amount: t.Amount}
			continue
		}

		if err := accounts.ApplyDebit(t.DebitAccountID, t.Amount); err != nil {
			outcomes[i] = err
			continue
		}
		if err := accounts.ApplyCredit(t.CreditAccountID, t.Amount); err != nil {
			outcomes[i] = err
		}
	}
}
