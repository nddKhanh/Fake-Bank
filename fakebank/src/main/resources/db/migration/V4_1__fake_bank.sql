CREATE TABLE bank_accounts (
  id             UUID PRIMARY KEY,
  account_number VARCHAR(50) NOT NULL UNIQUE,
  holder_name    VARCHAR(100) NOT NULL,
  balance        BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
  status         VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
);

CREATE TABLE bank_transactions (
  id                 UUID PRIMARY KEY,
  bank_reference     VARCHAR(50) NOT NULL UNIQUE,
  client_request_id  VARCHAR(100) NOT NULL UNIQUE,
  type               VARCHAR(30) NOT NULL,
  account_number     VARCHAR(50) NOT NULL,
  amount             BIGINT NOT NULL CHECK (amount > 0),
  currency           CHAR(3) NOT NULL,
  status             VARCHAR(20) NOT NULL,
  failure_code       VARCHAR(50),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at       TIMESTAMPTZ
);

CREATE INDEX ix_bank_tx_completed ON bank_transactions (completed_at);

CREATE TABLE bank_callback_outbox (
  id                  BIGSERIAL PRIMARY KEY,
  bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id),
  event_id            UUID NOT NULL UNIQUE,
  event_type          VARCHAR(50) NOT NULL,
  payload             JSONB NOT NULL,
  attempts            INT NOT NULL DEFAULT 0,
  next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  delivered_at        TIMESTAMPTZ
);

CREATE INDEX ix_bank_cb_due
  ON bank_callback_outbox (next_attempt_at)
  WHERE delivered_at IS NULL;

CREATE TABLE chaos_rules (
  id          SERIAL PRIMARY KEY,
  name        VARCHAR(50) NOT NULL UNIQUE,
  enabled     BOOLEAN NOT NULL DEFAULT FALSE,
  mode        VARCHAR(40) NOT NULL,
  probability NUMERIC(4,3) NOT NULL DEFAULT 1.0 CHECK (probability BETWEEN 0 AND 1),
  params      JSONB,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
