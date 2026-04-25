package replication

import (
	"fmt"
	"log/slog"

	"aequitas-ledger/internal/engine"
)

// PromoteToPrimary stops the replication loop and transitions the Follower replica to an active Primary Ledger engine.
func (f *Follower) PromoteToPrimary(cfg engine.Config) (*engine.Ledger, error) {
	slog.Info("Initiating Follower replica promotion to active Primary...", "lastLSN", f.LastLSN())

	// Step 1: Stop streaming replication loop
	f.Stop()

	// Step 2: Extract current accounts snapshot for logging and clear InitialAccounts
	accs := f.Accounts()
	cfg.InitialAccounts = nil

	// Step 3: Initialize new Ledger engine using local WAL
	ledger, err := engine.NewLedger(cfg, f.localWAL)
	if err != nil {
		return nil, fmt.Errorf("failed to promote replica to primary ledger: %w", err)
	}

	slog.Info("Follower replica successfully promoted to Primary!", "accounts", len(accs))
	return ledger, nil
}
