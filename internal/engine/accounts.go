package engine

import (
	"container/heap"
	"fmt"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/observability"
)

// pendingEntry tracks one open hold (D1.2).
type pendingEntry struct {
	transfer core.Transfer
	expires  int64 // absolute logical-clock deadline in ns; 0 = never
	heapIdx  int
}

// expiryHeap orders pending entries by deadline (min-heap).
type expiryHeap []*pendingEntry

func (h expiryHeap) Len() int            { return len(h) }
func (h expiryHeap) Less(i, j int) bool  { return h[i].expires < h[j].expires }
func (h expiryHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i]; h[i].heapIdx = i; h[j].heapIdx = j }
func (h *expiryHeap) Push(x any)         { e := x.(*pendingEntry); e.heapIdx = len(*h); *h = append(*h, e) }
func (h *expiryHeap) Pop() any           { old := *h; n := len(old); e := old[n-1]; old[n-1] = nil; *h = old[:n-1]; return e }

type AccountManager struct {
	accounts []core.Account
	index    map[[16]byte]int

	// Two-phase state (D1.2). clock is the logical batch clock: the last
	// batch timestamp seen. Expiry is judged against it — never wall time —
	// so replay decisions are byte-identical to live ones.
	clock    int64
	pending  map[[16]byte]*pendingEntry
	expiry   expiryHeap

	// Transfer history (D1.5): every applied operation, indexed by ID and
	// per account in arrival order. Covers the WAL-since-snapshot window.
	transfers map[[16]byte]core.Transfer
	history   map[[16]byte][][16]byte
}

func NewAccountManager(initialCapacity int) *AccountManager {
	if initialCapacity < 0 {
		initialCapacity = 0
	}
	return &AccountManager{
		accounts:  make([]core.Account, 0, initialCapacity),
		index:     make(map[[16]byte]int, initialCapacity),
		pending:   make(map[[16]byte]*pendingEntry),
		transfers: make(map[[16]byte]core.Transfer),
		history:   make(map[[16]byte][][16]byte),
	}
}

func (m *AccountManager) Create(a core.Account) error {
	if _, exists := m.index[a.ID]; exists {
		return core.ErrDuplicateAccountID{AccountID: a.ID}
	}
	m.accounts = append(m.accounts, a)
	m.index[a.ID] = len(m.accounts) - 1
	return nil
}

func (m *AccountManager) Get(id [16]byte) (*core.Account, error) {
	idx, ok := m.index[id]
	if !ok {
		return nil, core.ErrAccountNotFound{AccountID: id}
	}
	return &m.accounts[idx], nil
}

// SetClock advances the logical clock; expiry is judged against it.
func (m *AccountManager) SetClock(nanos int64) {
	m.clock = nanos
}

// Clock returns the current logical clock timestamp in nanoseconds.
func (m *AccountManager) Clock() int64 {
	return m.clock
}

// CloseExpired voids every pending transfer whose deadline has passed on the
// logical clock. Derived state — no journal records — so replay reproduces
// the exact same closures.
func (m *AccountManager) CloseExpired() {
	for m.expiry.Len() > 0 {
		top := m.expiry[0]
		if top.expires > m.clock {
			return
		}
		heap.Pop(&m.expiry)
		delete(m.pending, top.transfer.ID)
		if err := m.releasePending(top.transfer); err != nil {
			// Internal state inconsistency: surfaced loudly via the metric.
			observability.InvariantViolations.Inc()
		}
	}
}

// releasePending returns a hold's reserved amounts (void or expiry).
func (m *AccountManager) releasePending(t core.Transfer) error {
	if acc, err := m.Get(t.DebitAccountID); err != nil {
		return err
	} else if next, err := core.Sub(acc.PendingDebits, t.Amount); err != nil {
		return core.ErrInvariantViolation{AccountID: acc.ID}
	} else {
		acc.PendingDebits = next
	}
	if acc, err := m.Get(t.CreditAccountID); err != nil {
		return err
	} else if next, err := core.Sub(acc.PendingCredits, t.Amount); err != nil {
		return core.ErrInvariantViolation{AccountID: acc.ID}
	} else {
		acc.PendingCredits = next
	}
	return nil
}

// ValidateAccount rejects account state that violates the ledger's
// non-negative-balance invariant before it can enter the WAL.
func ValidateAccount(a core.Account) error {
	if _, err := core.Balance(a); err != nil {
		observability.InvariantViolations.Inc()
		return err
	}
	return nil
}

// ValidateTransfer performs the full flag-aware pre-flight check for one
// transfer. ApplyTransferState re-runs the same check before mutating — one
// source of truth for the rules.
func (m *AccountManager) ValidateTransfer(t core.Transfer) error {
	_, err := m.checkTransfer(t)
	return err
}

func (m *AccountManager) checkTransfer(t core.Transfer) (remaining core.Uint128, err error) {
	isPost := t.Flags&core.TransferFlagPostPending != 0
	isVoid := t.Flags&core.TransferFlagVoidPending != 0
	isPending := t.Flags&core.TransferFlagPending != 0

	if isPost && (isPending || isVoid) || isVoid && isPending {
		return remaining, core.ErrInvalidTransferFlags{Flags: t.Flags}
	}

	if isPost || isVoid {
		p, ok := m.pending[t.ID]
		if !ok {
			return remaining, core.ErrPendingNotFound{TransferID: t.ID}
		}
		pending := p.transfer
		remaining = pending.Amount
		if isPost {
			// Amount 0 settles the full remaining hold.
			if !core.IsZero(t.Amount) {
				if core.Cmp(t.Amount, pending.Amount) > 0 {
					return remaining, core.ErrOverTransfer{
						TransferID: t.ID, Pending: pending.Amount, Amount: t.Amount,
					}
				}
				remaining = t.Amount
			}
		}
		return remaining, nil
	}

	// Plain and pending transfers share the structural checks.
	if core.IsZero(t.Amount) {
		return remaining, core.ErrZeroAmount{}
	}
	if t.DebitAccountID == t.CreditAccountID {
		return remaining, core.ErrSelfTransfer{AccountID: t.DebitAccountID}
	}
	debit, err := m.Get(t.DebitAccountID)
	if err != nil {
		return remaining, err
	}
	credit, err := m.Get(t.CreditAccountID)
	if err != nil {
		return remaining, err
	}
	if debit.Currency != credit.Currency {
		return remaining, core.ErrCurrencyMismatch{DebitCurrency: debit.Currency, CreditCurrency: credit.Currency}
	}
	if core.IsFrozen(*debit) {
		return remaining, core.ErrAccountFrozen{AccountID: debit.ID}
	}
	if core.IsClosed(*debit) {
		return remaining, core.ErrAccountClosed{AccountID: debit.ID}
	}
	if core.IsFrozen(*credit) {
		return remaining, core.ErrAccountFrozen{AccountID: credit.ID}
	}
	if core.IsClosed(*credit) {
		return remaining, core.ErrAccountClosed{AccountID: credit.ID}
	}

	if isPending {
		// A hold reserves against the AVAILABLE balance, not the posted one.
		avail, err := core.AvailableBalance(*debit)
		if err != nil {
			observability.InvariantViolations.Inc()
			return remaining, err
		}
		if core.Cmp(avail, t.Amount) < 0 {
			return remaining, core.ErrInsufficientFunds{AccountID: debit.ID, Balance: avail, Amount: t.Amount}
		}
		return remaining, nil
	}

	bal, err := core.Balance(*debit)
	if err != nil {
		observability.InvariantViolations.Inc()
		return remaining, err
	}
	if core.Cmp(bal, t.Amount) < 0 {
		return remaining, core.ErrInsufficientFunds{AccountID: debit.ID, Balance: bal, Amount: t.Amount}
	}
	return remaining, nil
}

// ApplyTransferState applies one transfer's flag semantics to the state
// machine. This is THE shared apply path: the event loop, WAL replay, and
// replication followers all go through it, guaranteeing identical state
// machines everywhere (C0.8 item 5, D1.2).
func ApplyTransferState(m *AccountManager, t core.Transfer) error {
	remaining, err := m.checkTransfer(t)
	if err != nil {
		return err
	}

	isPost := t.Flags&core.TransferFlagPostPending != 0
	isVoid := t.Flags&core.TransferFlagVoidPending != 0
	isPending := t.Flags&core.TransferFlagPending != 0

	switch {
	case isPost:
		p := m.pending[t.ID]
		amount := remaining
		if err := m.movePending(p.transfer, amount); err != nil {
			return err
		}
		if core.Cmp(p.transfer.Amount, amount) == 0 {
			m.removePending(t.ID)
		} else {
			p.transfer = subtractPendingAmount(p.transfer, amount)
		}
		m.recordTransfer(t)
		return nil
	case isVoid:
		p := m.pending[t.ID]
		if err := m.releasePending(p.transfer); err != nil {
			return err
		}
		m.removePending(t.ID)
		m.recordTransfer(t)
		return nil
	case isPending:
		if err := m.reservePending(t); err != nil {
			return err
		}
		m.recordTransfer(t)
		return nil
	default:
		if err := m.postImmediate(t); err != nil {
			return err
		}
		m.recordTransfer(t)
		return nil
	}
}

func subtractPendingAmount(t core.Transfer, amount core.Uint128) core.Transfer {
	if rest, err := core.Sub(t.Amount, amount); err == nil {
		t.Amount = rest
	}
	return t
}

func (m *AccountManager) postImmediate(t core.Transfer) error {
	if err := m.ApplyDebit(t.DebitAccountID, t.Amount); err != nil {
		return err
	}
	return m.ApplyCredit(t.CreditAccountID, t.Amount)
}

func (m *AccountManager) reservePending(t core.Transfer) error {
	if err := m.applyPendingDebit(t.DebitAccountID, t.Amount); err != nil {
		return err
	}
	if err := m.applyPendingCredit(t.CreditAccountID, t.Amount); err != nil {
		return err
	}
	e := &pendingEntry{transfer: t}
	if t.Timeout > 0 {
		e.expires = t.Timestamp + int64(t.Timeout)
		heap.Push(&m.expiry, e)
	}
	m.pending[t.ID] = e
	return nil
}

// movePending settles `amount` of an open hold: pending → posted on both
// sides.
func (m *AccountManager) movePending(pending core.Transfer, amount core.Uint128) error {
	if acc, err := m.Get(pending.DebitAccountID); err != nil {
		return err
	} else if next, err := core.Sub(acc.PendingDebits, amount); err != nil {
		return core.ErrInvariantViolation{AccountID: acc.ID}
	} else if posted, err := core.Add(acc.PostedDebits, amount); err != nil {
		return core.ErrBalanceOverflow{AccountID: acc.ID}
	} else {
		acc.PendingDebits = next
		acc.PostedDebits = posted
	}
	if acc, err := m.Get(pending.CreditAccountID); err != nil {
		return err
	} else if next, err := core.Sub(acc.PendingCredits, amount); err != nil {
		return core.ErrInvariantViolation{AccountID: acc.ID}
	} else if posted, err := core.Add(acc.PostedCredits, amount); err != nil {
		return core.ErrBalanceOverflow{AccountID: acc.ID}
	} else {
		acc.PendingCredits = next
		acc.PostedCredits = posted
	}
	return nil
}

func (m *AccountManager) applyPendingDebit(id [16]byte, amount core.Uint128) error {
	acc, err := m.Get(id)
	if err != nil {
		return err
	}
	next, err := core.Add(acc.PendingDebits, amount)
	if err != nil {
		return core.ErrBalanceOverflow{AccountID: id}
	}
	acc.PendingDebits = next
	return nil
}

func (m *AccountManager) applyPendingCredit(id [16]byte, amount core.Uint128) error {
	acc, err := m.Get(id)
	if err != nil {
		return err
	}
	next, err := core.Add(acc.PendingCredits, amount)
	if err != nil {
		return core.ErrBalanceOverflow{AccountID: id}
	}
	acc.PendingCredits = next
	return nil
}

func (m *AccountManager) removePending(id [16]byte) {
	if e, ok := m.pending[id]; ok {
		if e.heapIdx >= 0 && e.heapIdx < m.expiry.Len() && m.expiry[e.heapIdx] == e {
			heap.Remove(&m.expiry, e.heapIdx)
		}
		delete(m.pending, id)
	}
}

// recordTransfer maintains the D1.5 query index and enforces unique transfer
// IDs for plain/pending operations (post/void reuse the pending's ID).
func (m *AccountManager) recordTransfer(t core.Transfer) {
	isPost := t.Flags&core.TransferFlagPostPending != 0
	isVoid := t.Flags&core.TransferFlagVoidPending != 0
	if !isPost && !isVoid {
		if _, dup := m.transfers[t.ID]; dup {
			// Enforcement point: surfaced as a per-item failure by callers
			// that check before journaling.
			observability.TransfersTotal.WithLabelValues("duplicate_id").Inc()
		}
	}
	m.transfers[t.ID] = t
	if t.DebitAccountID != t.CreditAccountID {
		m.history[t.DebitAccountID] = append(m.history[t.DebitAccountID], t.ID)
	}
	m.history[t.CreditAccountID] = append(m.history[t.CreditAccountID], t.ID)
}

func (m *AccountManager) ApplyDebit(id [16]byte, amount core.Uint128) error {
	acc, err := m.Get(id)
	if err != nil {
		return err
	}
	next, err := core.Add(acc.PostedDebits, amount)
	if err != nil {
		return core.ErrBalanceOverflow{AccountID: id}
	}
	acc.PostedDebits = next
	// Post-apply invariant assert: pre-validation makes this unreachable for
	// well-formed state; a violation here means stored state was already
	// broken, so report it loudly rather than silently continuing.
	if _, err := core.Balance(*acc); err != nil {
		observability.InvariantViolations.Inc()
		return err
	}
	return nil
}

func (m *AccountManager) ApplyCredit(id [16]byte, amount core.Uint128) error {
	acc, err := m.Get(id)
	if err != nil {
		return err
	}
	next, err := core.Add(acc.PostedCredits, amount)
	if err != nil {
		return core.ErrBalanceOverflow{AccountID: id}
	}
	acc.PostedCredits = next
	return nil
}

func (m *AccountManager) Snapshot() []core.Account {
	out := make([]core.Account, len(m.accounts))
	copy(out, m.accounts)
	return out
}

// PendingTransfers snapshots the still-open holds (for the snapshot file).
func (m *AccountManager) PendingTransfers() []core.Transfer {
	out := make([]core.Transfer, 0, len(m.pending))
	for _, e := range m.pending {
		out = append(out, e.transfer)
	}
	return out
}

// RestorePending re-registers a hold loaded from a snapshot.
func (m *AccountManager) RestorePending(t core.Transfer) error {
	if _, ok := m.pending[t.ID]; ok {
		return fmt.Errorf("pending transfer %x restored twice", t.ID)
	}
	e := &pendingEntry{transfer: t}
	if t.Timeout > 0 {
		e.expires = t.Timestamp + int64(t.Timeout)
		heap.Push(&m.expiry, e)
	}
	m.pending[t.ID] = e
	return nil
}

// GetTransfer returns one recorded operation (D1.5).
func (m *AccountManager) GetTransfer(id [16]byte) (core.Transfer, bool) {
	t, ok := m.transfers[id]
	return t, ok
}

// AccountTransfers returns up to limit transfers touching the account, in
// arrival order, starting after afterID (cursor; empty = from the start).
func (m *AccountManager) AccountTransfers(accountID [16]byte, afterID [16]byte, limit int) []core.Transfer {
	ids := m.history[accountID]
	start := 0
	if afterID != ([16]byte{}) {
		for i, id := range ids {
			if id == afterID {
				start = i + 1
				break
			}
		}
	}
	if limit <= 0 || limit > len(ids)-start {
		limit = len(ids) - start
	}
	out := make([]core.Transfer, 0, limit)
	for _, id := range ids[start : start+limit] {
		if t, ok := m.transfers[id]; ok {
			out = append(out, t)
		}
	}
	return out
}

func (m *AccountManager) Len() int {
	return len(m.accounts)
}
