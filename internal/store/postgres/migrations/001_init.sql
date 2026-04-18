CREATE TABLE IF NOT EXISTS accounts (
    id VARCHAR(32) PRIMARY KEY,
    currency VARCHAR(4) NOT NULL,
    posted_debits NUMERIC(39, 0) NOT NULL DEFAULT 0,
    posted_credits NUMERIC(39, 0) NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transfers (
    id VARCHAR(32) PRIMARY KEY,
    debit_account_id VARCHAR(32) NOT NULL REFERENCES accounts(id),
    credit_account_id VARCHAR(32) NOT NULL REFERENCES accounts(id),
    amount NUMERIC(39, 0) NOT NULL,
    idempotency_key BYTEA UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transfers_debit ON transfers(debit_account_id);
CREATE INDEX IF NOT EXISTS idx_transfers_credit ON transfers(credit_account_id);
