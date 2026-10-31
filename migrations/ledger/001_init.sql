CREATE TABLE accounts (
    id         uuid PRIMARY KEY,
    owner_id   text        NOT NULL,
    currency   char(3)     NOT NULL,
    balance    bigint      NOT NULL DEFAULT 0,
    is_system  boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- System accounts are the "outside world" side of deposits and
    -- withdrawals, so only they are allowed to go negative.
    CONSTRAINT accounts_balance_non_negative CHECK (is_system OR balance >= 0)
);

CREATE INDEX accounts_owner_idx ON accounts (owner_id);
CREATE UNIQUE INDEX accounts_system_currency_idx ON accounts (currency) WHERE is_system;

CREATE TABLE transfers (
    id              uuid PRIMARY KEY,
    kind            text        NOT NULL CHECK (kind IN ('deposit', 'withdrawal', 'transfer')),
    from_account_id uuid        NOT NULL REFERENCES accounts (id),
    to_account_id   uuid        NOT NULL REFERENCES accounts (id),
    amount          bigint      NOT NULL CHECK (amount > 0),
    currency        char(3)     NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CHECK (from_account_id <> to_account_id)
);

CREATE TABLE entries (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_id   uuid        NOT NULL REFERENCES transfers (id),
    account_id    uuid        NOT NULL REFERENCES accounts (id),
    amount        bigint      NOT NULL CHECK (amount <> 0),
    balance_after bigint      NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Serves the account statement query: WHERE account_id = $1 AND id < $2 ORDER BY id DESC.
CREATE INDEX entries_account_id_idx ON entries (account_id, id DESC);
CREATE INDEX entries_transfer_idx ON entries (transfer_id);

-- The key row is inserted first in the transfer transaction, before the
-- transfer itself exists, so transfer_id is filled in at the end of it.
CREATE TABLE idempotency_keys (
    key           text PRIMARY KEY,
    request_hash  bytea       NOT NULL,
    transfer_id   uuid        REFERENCES transfers (id),
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic        text        NOT NULL,
    message_key  text        NOT NULL,
    payload      jsonb       NOT NULL,
    headers      jsonb       NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- Partial index: the relay only ever looks at unpublished rows, and this
-- index stays tiny no matter how large the table grows.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

INSERT INTO accounts (id, owner_id, currency, is_system)
SELECT gen_random_uuid(), 'system', c, true
FROM unnest(ARRAY['KZT', 'USD', 'EUR', 'RUB']) AS c;
