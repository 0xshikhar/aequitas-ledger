package engine

import "aequitas-ledger/internal/core"

// SeedAccount is intended for deterministic test/bootstrap setup before high write concurrency begins.
func (l *Ledger) SeedAccount(a core.Account) error {
	return l.accounts.Create(a)
}
