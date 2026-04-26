package engine

import "aequitas-ledger/internal/core"

type AccountManager struct {
	accounts []core.Account
	index    map[[16]byte]int
}

func NewAccountManager(initialCapacity int) *AccountManager {
	if initialCapacity < 0 {
		initialCapacity = 0
	}
	return &AccountManager{
		accounts: make([]core.Account, 0, initialCapacity),
		index:    make(map[[16]byte]int, initialCapacity),
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

func (m *AccountManager) ValidateTransfer(t core.Transfer) error {
	if core.IsZero(t.Amount) {
		return core.ErrZeroAmount{}
	}
	if t.DebitAccountID == t.CreditAccountID {
		return core.ErrSelfTransfer{AccountID: t.DebitAccountID}
	}

	debit, err := m.Get(t.DebitAccountID)
	if err != nil {
		return err
	}
	credit, err := m.Get(t.CreditAccountID)
	if err != nil {
		return err
	}

	if debit.Currency != credit.Currency {
		return core.ErrCurrencyMismatch{DebitCurrency: debit.Currency, CreditCurrency: credit.Currency}
	}
	if core.IsFrozen(*debit) {
		return core.ErrAccountFrozen{AccountID: debit.ID}
	}
	if core.IsClosed(*debit) {
		return core.ErrAccountClosed{AccountID: debit.ID}
	}
	if core.IsFrozen(*credit) {
		return core.ErrAccountFrozen{AccountID: credit.ID}
	}
	if core.IsClosed(*credit) {
		return core.ErrAccountClosed{AccountID: credit.ID}
	}

	bal := core.Balance(*debit)
	if core.Cmp(bal, t.Amount) < 0 {
		return core.ErrInsufficientFunds{AccountID: debit.ID, Balance: bal, Amount: t.Amount}
	}

	return nil
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

func (m *AccountManager) Len() int {
	return len(m.accounts)
}
