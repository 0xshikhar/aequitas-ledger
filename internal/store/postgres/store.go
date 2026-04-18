package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"aequitas-ledger/internal/core"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, connString string) (*Store, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connString: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	s := &Store{pool: pool}
	if err := s.InitSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) InitSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS accounts (
		id VARCHAR(32) PRIMARY KEY,
		currency VARCHAR(4) NOT NULL,
		posted_debits NUMERIC(39, 0) NOT NULL DEFAULT 0,
		posted_credits NUMERIC(39, 0) NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS transfers (
		id VARCHAR(32) PRIMARY KEY,
		debit_account_id VARCHAR(32) NOT NULL REFERENCES accounts(id),
		credit_account_id VARCHAR(32) NOT NULL REFERENCES accounts(id),
		amount NUMERIC(39, 0) NOT NULL,
		idempotency_key BYTEA UNIQUE
	);
	`
	_, err := s.pool.Exec(ctx, schema)
	return err
}

func (s *Store) CreateAccount(ctx context.Context, acc core.Account) error {
	idStr := fmt.Sprintf("%x", acc.ID[:])
	curr := strings.TrimRight(string(acc.Currency[:]), "\x00")
	debitsStr := core.String(acc.PostedDebits)
	creditsStr := core.String(acc.PostedCredits)

	_, err := s.pool.Exec(ctx,
		`INSERT INTO accounts (id, currency, posted_debits, posted_credits) VALUES ($1, $2, $3, $4)`,
		idStr, curr, debitsStr, creditsStr,
	)
	return err
}

func (s *Store) GetAccount(ctx context.Context, id [16]byte) (core.Account, error) {
	idStr := fmt.Sprintf("%x", id[:])
	var curr string
	var debitsStr, creditsStr string

	err := s.pool.QueryRow(ctx,
		`SELECT currency, posted_debits, posted_credits FROM accounts WHERE id = $1`, idStr,
	).Scan(&curr, &debitsStr, &creditsStr)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return core.Account{}, core.ErrAccountNotFound{AccountID: id}
		}
		return core.Account{}, err
	}

	var currency [4]byte
	copy(currency[:], []byte(curr))
	deb, _ := core.FromString(debitsStr)
	cred, _ := core.FromString(creditsStr)

	return core.Account{
		ID:            id,
		Currency:      currency,
		PostedDebits:  deb,
		PostedCredits: cred,
	}, nil
}

func (s *Store) CreateTransfer(ctx context.Context, tr core.Transfer) (core.Transfer, error) {
	if core.IsZero(tr.Amount) {
		return core.Transfer{}, core.ErrZeroAmount{}
	}
	if tr.DebitAccountID == tr.CreditAccountID {
		return core.Transfer{}, core.ErrSelfTransfer{AccountID: tr.DebitAccountID}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return core.Transfer{}, err
	}
	defer tx.Rollback(ctx)

	// Lock accounts in deterministic ID order to prevent PostgreSQL deadlocks
	id1 := fmt.Sprintf("%x", tr.DebitAccountID[:])
	id2 := fmt.Sprintf("%x", tr.CreditAccountID[:])
	first, second := id1, id2
	if first > second {
		first, second = second, first
	}

	var acc1Debits, acc1Credits string
	err = tx.QueryRow(ctx, `SELECT posted_debits, posted_credits FROM accounts WHERE id = $1 FOR UPDATE`, first).Scan(&acc1Debits, &acc1Credits)
	if err != nil {
		return core.Transfer{}, fmt.Errorf("debit/credit account not found: %w", err)
	}

	var acc2Debits, acc2Credits string
	err = tx.QueryRow(ctx, `SELECT posted_debits, posted_credits FROM accounts WHERE id = $1 FOR UPDATE`, second).Scan(&acc2Debits, &acc2Credits)
	if err != nil {
		return core.Transfer{}, fmt.Errorf("debit/credit account not found: %w", err)
	}

	// Verify funds for debit account
	var debitDebitsStr, debitCreditsStr string
	if id1 == first {
		debitDebitsStr, debitCreditsStr = acc1Debits, acc1Credits
	} else {
		debitDebitsStr, debitCreditsStr = acc2Debits, acc2Credits
	}

	debDebits, _ := core.FromString(debitDebitsStr)
	debCredits, _ := core.FromString(debitCreditsStr)
	debAcc := core.Account{ID: tr.DebitAccountID, PostedDebits: debDebits, PostedCredits: debCredits}
	bal := core.Balance(debAcc)

	if core.Cmp(bal, tr.Amount) < 0 {
		return core.Transfer{}, core.ErrInsufficientFunds{AccountID: tr.DebitAccountID, Balance: bal, Amount: tr.Amount}
	}

	trIDStr := fmt.Sprintf("%x", tr.ID[:])
	amtStr := core.String(tr.Amount)
	_, err = tx.Exec(ctx,
		`INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, idempotency_key) VALUES ($1, $2, $3, $4, $5)`,
		trIDStr, id1, id2, amtStr, tr.IdempotencyKey[:],
	)
	if err != nil {
		return core.Transfer{}, err
	}

	// Update debit account debits
	_, err = tx.Exec(ctx, `UPDATE accounts SET posted_debits = posted_debits + $1 WHERE id = $2`, amtStr, id1)
	if err != nil {
		return core.Transfer{}, err
	}

	// Update credit account credits
	_, err = tx.Exec(ctx, `UPDATE accounts SET posted_credits = posted_credits + $1 WHERE id = $2`, amtStr, id2)
	if err != nil {
		return core.Transfer{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return core.Transfer{}, err
	}

	return tr, nil
}

// CheckInvariant verifies sum(posted_debits) == sum(posted_credits) in Postgres
func (s *Store) CheckInvariant(ctx context.Context) (bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT posted_debits, posted_credits FROM accounts`)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	totalDebits := new(big.Int)
	totalCredits := new(big.Int)

	for rows.Next() {
		var debStr, credStr string
		if err := rows.Scan(&debStr, &credStr); err != nil {
			return false, err
		}
		deb, _ := new(big.Int).SetString(debStr, 10)
		cred, _ := new(big.Int).SetString(credStr, 10)
		totalDebits.Add(totalDebits, deb)
		totalCredits.Add(totalCredits, cred)
	}

	return totalDebits.Cmp(totalCredits) == 0, nil
}

// Reset clears accounts and transfers tables for test isolation
func (s *Store) Reset(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE TABLE transfers, accounts CASCADE`)
	return err
}
