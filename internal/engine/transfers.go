package engine

import (
	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/observability"
)

type accountSnapshot struct {
	id      [16]byte
	account core.Account
	loaded  bool
}

type pendingSnapshot struct {
	id     [16]byte
	state  pendingState
	loaded bool
}

type pendingState struct {
	transfer core.Transfer
	deleted  bool
}

type batchValidator struct {
	accounts *AccountManager
	staged   map[[16]byte]*core.Account
	pending  map[[16]byte]pendingState
	seenIDs  map[[16]byte]int

	accSnaps  []accountSnapshot
	pendSnaps []pendingSnapshot
	accSeen   map[[16]byte]bool
	pendSeen  map[[16]byte]bool
}

func newBatchValidator(accounts *AccountManager, capacity int) *batchValidator {
	return &batchValidator{
		accounts: accounts,
		staged:   make(map[[16]byte]*core.Account, capacity),
		pending:  make(map[[16]byte]pendingState),
		seenIDs:  make(map[[16]byte]int, capacity),
		accSeen:  make(map[[16]byte]bool),
		pendSeen: make(map[[16]byte]bool),
	}
}

func (v *batchValidator) beginChain() {
	v.accSnaps = v.accSnaps[:0]
	v.pendSnaps = v.pendSnaps[:0]
	clear(v.accSeen)
	clear(v.pendSeen)
}

func (v *batchValidator) rollbackChain() {
	for i := len(v.accSnaps) - 1; i >= 0; i-- {
		s := v.accSnaps[i]
		if !s.loaded {
			delete(v.staged, s.id)
		} else {
			*v.staged[s.id] = s.account
		}
	}
	for i := len(v.pendSnaps) - 1; i >= 0; i-- {
		s := v.pendSnaps[i]
		if !s.loaded {
			delete(v.pending, s.id)
		} else {
			v.pending[s.id] = s.state
		}
	}
}

func (v *batchValidator) getAccount(id [16]byte) (*core.Account, error) {
	if a, ok := v.staged[id]; ok {
		return a, nil
	}
	acc, err := v.accounts.Get(id)
	if err != nil {
		return nil, err
	}
	cloned := *acc
	v.staged[id] = &cloned
	return &cloned, nil
}

func (v *batchValidator) snapshotAccount(id [16]byte) {
	if v.accSeen[id] {
		return
	}
	v.accSeen[id] = true
	if a, ok := v.staged[id]; ok {
		v.accSnaps = append(v.accSnaps, accountSnapshot{id: id, account: *a, loaded: true})
	} else {
		v.accSnaps = append(v.accSnaps, accountSnapshot{id: id, loaded: false})
	}
}

func (v *batchValidator) snapshotPending(id [16]byte) {
	if v.pendSeen[id] {
		return
	}
	v.pendSeen[id] = true
	if p, ok := v.pending[id]; ok {
		v.pendSnaps = append(v.pendSnaps, pendingSnapshot{id: id, state: p, loaded: true})
	} else {
		v.pendSnaps = append(v.pendSnaps, pendingSnapshot{id: id, loaded: false})
	}
}

func (v *batchValidator) validateAndApply(t core.Transfer, index int) error {
	knownFlags := core.TransferFlagPending | core.TransferFlagPostPending | core.TransferFlagVoidPending | core.TransferFlagLinked
	if t.Flags &^ knownFlags != 0 {
		return core.ErrInvalidTransferFlags{Flags: t.Flags}
	}

	isPost := t.Flags&core.TransferFlagPostPending != 0
	isVoid := t.Flags&core.TransferFlagVoidPending != 0
	isPending := t.Flags&core.TransferFlagPending != 0

	if isPost && (isPending || isVoid) || isVoid && isPending {
		return core.ErrInvalidTransferFlags{Flags: t.Flags}
	}

	if !isPost && !isVoid {
		if _, dup := v.seenIDs[t.ID]; dup {
			return core.ErrDuplicateTransferID{TransferID: t.ID}
		}
		if _, dup := v.accounts.GetTransfer(t.ID); dup {
			return core.ErrDuplicateTransferID{TransferID: t.ID}
		}
		v.seenIDs[t.ID] = index
	}

	if isPost || isVoid {
		var pending core.Transfer
		if st, ok := v.pending[t.ID]; ok {
			if st.deleted {
				return core.ErrPendingNotFound{TransferID: t.ID}
			}
			pending = st.transfer
		} else if p, ok := v.accounts.pending[t.ID]; ok {
			pending = p.transfer
		} else {
			return core.ErrPendingNotFound{TransferID: t.ID}
		}

		if t.Ledger != 0 && t.Ledger != pending.Ledger {
			return core.ErrLedgerMismatch{
				TransferLedger: t.Ledger,
				DebitLedger:    pending.Ledger,
				CreditLedger:   pending.Ledger,
			}
		}

		amount := pending.Amount
		if isPost && !core.IsZero(t.Amount) {
			if core.Cmp(t.Amount, pending.Amount) > 0 {
				return core.ErrOverTransfer{
					TransferID: t.ID, Pending: pending.Amount, Amount: t.Amount,
				}
			}
			amount = t.Amount
		}

		v.snapshotPending(t.ID)
		v.snapshotAccount(pending.DebitAccountID)
		v.snapshotAccount(pending.CreditAccountID)

		deb, err := v.getAccount(pending.DebitAccountID)
		if err != nil {
			return err
		}
		cred, err := v.getAccount(pending.CreditAccountID)
		if err != nil {
			return err
		}

		if isPost {
			nextPendDeb, err := core.Sub(deb.PendingDebits, amount)
			if err != nil {
				return core.ErrInvariantViolation{AccountID: deb.ID}
			}
			nextPostDeb, err := core.Add(deb.PostedDebits, amount)
			if err != nil {
				return core.ErrBalanceOverflow{AccountID: deb.ID}
			}
			deb.PendingDebits = nextPendDeb
			deb.PostedDebits = nextPostDeb

			nextPendCred, err := core.Sub(cred.PendingCredits, amount)
			if err != nil {
				return core.ErrInvariantViolation{AccountID: cred.ID}
			}
			nextPostCred, err := core.Add(cred.PostedCredits, amount)
			if err != nil {
				return core.ErrBalanceOverflow{AccountID: cred.ID}
			}
			cred.PendingCredits = nextPendCred
			cred.PostedCredits = nextPostCred

			v.staged[deb.ID] = deb
			v.staged[cred.ID] = cred

			if core.Cmp(pending.Amount, amount) == 0 {
				v.pending[t.ID] = pendingState{deleted: true}
			} else {
				newPend := pending
				newPend.Amount, _ = core.Sub(pending.Amount, amount)
				v.pending[t.ID] = pendingState{transfer: newPend}
			}
			return nil
		} else { // isVoid
			nextPendDeb, err := core.Sub(deb.PendingDebits, pending.Amount)
			if err != nil {
				return core.ErrInvariantViolation{AccountID: deb.ID}
			}
			deb.PendingDebits = nextPendDeb

			nextPendCred, err := core.Sub(cred.PendingCredits, pending.Amount)
			if err != nil {
				return core.ErrInvariantViolation{AccountID: cred.ID}
			}
			cred.PendingCredits = nextPendCred

			v.staged[deb.ID] = deb
			v.staged[cred.ID] = cred
			v.pending[t.ID] = pendingState{deleted: true}
			return nil
		}
	}

	// Plain and pending transfers
	if core.IsZero(t.Amount) {
		return core.ErrZeroAmount{}
	}
	if t.DebitAccountID == t.CreditAccountID {
		return core.ErrSelfTransfer{AccountID: t.DebitAccountID}
	}

	v.snapshotAccount(t.DebitAccountID)
	v.snapshotAccount(t.CreditAccountID)

	deb, err := v.getAccount(t.DebitAccountID)
	if err != nil {
		return err
	}
	cred, err := v.getAccount(t.CreditAccountID)
	if err != nil {
		return err
	}

	if deb.Currency != cred.Currency {
		return core.ErrCurrencyMismatch{DebitCurrency: deb.Currency, CreditCurrency: cred.Currency}
	}
	if deb.Ledger != t.Ledger || cred.Ledger != t.Ledger {
		return core.ErrLedgerMismatch{
			TransferLedger: t.Ledger,
			DebitLedger:    deb.Ledger,
			CreditLedger:   cred.Ledger,
		}
	}
	if core.IsFrozen(*deb) {
		return core.ErrAccountFrozen{AccountID: deb.ID}
	}
	if core.IsClosed(*deb) {
		return core.ErrAccountClosed{AccountID: deb.ID}
	}
	if core.IsFrozen(*cred) {
		return core.ErrAccountFrozen{AccountID: cred.ID}
	}
	if core.IsClosed(*cred) {
		return core.ErrAccountClosed{AccountID: cred.ID}
	}

	if isPending {
		avail, err := core.AvailableBalance(*deb)
		if err != nil {
			return err
		}
		if core.Cmp(avail, t.Amount) < 0 {
			return core.ErrInsufficientFunds{AccountID: deb.ID, Balance: avail, Amount: t.Amount}
		}
		nextPendDeb, err := core.Add(deb.PendingDebits, t.Amount)
		if err != nil {
			return core.ErrBalanceOverflow{AccountID: deb.ID}
		}
		nextPendCred, err := core.Add(cred.PendingCredits, t.Amount)
		if err != nil {
			return core.ErrBalanceOverflow{AccountID: cred.ID}
		}
		deb.PendingDebits = nextPendDeb
		cred.PendingCredits = nextPendCred
		v.staged[deb.ID] = deb
		v.staged[cred.ID] = cred

		v.snapshotPending(t.ID)
		v.pending[t.ID] = pendingState{transfer: t}
		return nil
	}

	// Plain transfer
	bal, err := core.Balance(*deb)
	if err != nil {
		return err
	}
	if core.Cmp(bal, t.Amount) < 0 {
		return core.ErrInsufficientFunds{AccountID: deb.ID, Balance: bal, Amount: t.Amount}
	}
	nextPostDeb, err := core.Add(deb.PostedDebits, t.Amount)
	if err != nil {
		return core.ErrBalanceOverflow{AccountID: deb.ID}
	}
	nextPostCred, err := core.Add(cred.PostedCredits, t.Amount)
	if err != nil {
		return core.ErrBalanceOverflow{AccountID: cred.ID}
	}
	deb.PostedDebits = nextPostDeb
	cred.PostedCredits = nextPostCred
	v.staged[deb.ID] = deb
	v.staged[cred.ID] = cred
	return nil
}

// ValidateBatch performs linked-chain-aware pre-flight checks per event (D1.3).
// If any transfer in a linked chain fails, none of the transfers in that chain
// execute or enter the WAL. Unlinked transfers commit or fail independently.
func ValidateBatch(events []TransferEvent, accounts *AccountManager) []error {
	outcomes := make([]error, len(events))
	if len(events) == 0 {
		return outcomes
	}

	validator := newBatchValidator(accounts, len(events)*2)

	chainStart := 0
	for i := 0; i < len(events); i++ {
		isLinked := events[i].Transfer.Flags&core.TransferFlagLinked != 0

		if i == chainStart {
			validator.beginChain()
		}

		err := validator.validateAndApply(events[i].Transfer, i)
		if err != nil {
			outcomes[i] = err

			// Find chainEnd: scan forward while isLinked
			chainEnd := i
			for chainEnd < len(events) && events[chainEnd].Transfer.Flags&core.TransferFlagLinked != 0 {
				chainEnd++
			}
			if chainEnd >= len(events) {
				chainEnd = len(events) - 1
			}

			validator.rollbackChain()

			for k := chainStart; k <= chainEnd; k++ {
				if k != i {
					outcomes[k] = core.ErrLinkedChainFailed{
						FailedTransferID: events[i].Transfer.ID,
						Index:            i,
						Reason:           err.Error(),
					}
				}
			}

			i = chainEnd
			chainStart = i + 1
			continue
		}

		if !isLinked {
			for k := chainStart; k <= i; k++ {
				outcomes[k] = nil
			}
			chainStart = i + 1
			continue
		}

		if i == len(events)-1 {
			// Dangling linked transfer at end of batch
			validator.rollbackChain()
			outcomes[i] = core.ErrLinkedChainOpen{
				TransferID: events[i].Transfer.ID,
				Index:      i,
			}
			for k := chainStart; k < i; k++ {
				outcomes[k] = core.ErrLinkedChainFailed{
					FailedTransferID: events[i].Transfer.ID,
					Index:            i,
					Reason:           outcomes[i].Error(),
				}
			}
			break
		}
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
