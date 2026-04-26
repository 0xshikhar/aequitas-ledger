package engine

import (
	"aequitas-ledger/internal/observability"
)

// ValidateBatch performs the flag-aware pre-flight check per event. Only
// events that pass are journaled; ApplyTransferState re-runs the identical
// check before mutating (one rules implementation, two call sites).
func ValidateBatch(events []TransferEvent, accounts *AccountManager) []error {
	outcomes := make([]error, len(events))
	for i := range events {
		outcomes[i] = accounts.ValidateTransfer(events[i].Transfer)
	}
	return outcomes
}

// ApplyBatch applies pre-validated events to the state machine in strict
// slice order. The WAL is already durable by the time this runs; any failure
// here is a state-machine-level rejection (the record replays identically).
func ApplyBatch(events []TransferEvent, outcomes []error, accounts *AccountManager) {
	for i := range events {
		if outcomes[i] != nil {
			continue
		}
		if err := ApplyTransferState(accounts, events[i].Transfer); err != nil {
			outcomes[i] = err
			observability.InvariantViolations.Inc()
		}
	}
}
