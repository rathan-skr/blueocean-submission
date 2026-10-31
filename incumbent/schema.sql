-- Settlement ledger schema (incumbent v0.9, PostgreSQL 15, behind PgBouncer in transaction mode)

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id      text PRIMARY KEY,
    currency         char(3) NOT NULL,
    reserve          double precision NOT NULL DEFAULT 0,
    bank_account     text NOT NULL
);

-- one row per applied ledger event; balances are derived from these
CREATE TABLE IF NOT EXISTS ledger_entries (
    id               bigserial PRIMARY KEY,
    merchant_id      text NOT NULL REFERENCES merchants(merchant_id),
    event_id         text NOT NULL,
    event_type       text NOT NULL,
    event_time       timestamptz NOT NULL,
    bucket           text NOT NULL,              -- 'settled' | 'held'
    amount           double precision NOT NULL   -- major units (e.g. dollars)
);
CREATE INDEX IF NOT EXISTS ledger_entries_event_time ON ledger_entries (event_time);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id         text PRIMARY KEY,
    processed_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payouts (
    payout_id        text PRIMARY KEY,           -- = the PAYOUT_REQUEST event_id
    merchant_id      text NOT NULL,
    amount           double precision NOT NULL,
    status           text NOT NULL,              -- accepted | paid | failed | rejected
    bank_payout_id   text,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- transactional outbox: rows are inserted in the same transaction as the payout
CREATE TABLE IF NOT EXISTS payout_outbox (
    id               bigserial PRIMARY KEY,
    payout_id        text NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS platform_fees (
    currency         char(3) PRIMARY KEY,
    total            double precision NOT NULL DEFAULT 0
);
