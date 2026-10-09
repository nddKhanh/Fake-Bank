-- Chapter 2, step 1.1: schema only; no seed or business logic.
CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    owner_ref VARCHAR(64) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'VND',
    balance BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE transfers (
    id UUID PRIMARY KEY,
    from_account_id UUID NOT NULL REFERENCES accounts(id),
    to_account_id UUID NOT NULL REFERENCES accounts(id),
    amount BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
