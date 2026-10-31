-- Settlement ledger schema. Idempotent: every service runs it on start-up (guarded by an advisory lock).
-- All money is bigint minor units. Nothing here is ever a float.

-- One row per merchant (= one BOSP channel). last_seq is the exactly-once guard AND the reader's
-- resume point: an event with channel seq n is applied only if last_seq = n - 1, in the same
-- transaction that writes its effects.
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id   text PRIMARY KEY,
    currency      char(3),
    reserve       bigint  NOT NULL DEFAULT 0,
    bank_account  text,
    settled       bigint  NOT NULL DEFAULT 0,
    held          bigint  NOT NULL DEFAULT 0,
    fees          bigint  NOT NULL DEFAULT 0,   -- platform fees earned on this merchant (sharded fee counter)
    last_seq      bigint  NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 70);                       -- room for HOT updates: this row changes every batch

-- Append-only journal: one row per DATA event (applied, rejected or ignored), carrying the balance
-- AFTER the event. A point-in-time balance is one index probe: the last row with event_time <= as_of.
CREATE TABLE IF NOT EXISTS ledger_entries (
    merchant_id    text        NOT NULL,
    seq            bigint      NOT NULL,
    event_id       text        NOT NULL,
    event_type     text        NOT NULL,
    event_time     timestamptz NOT NULL,
    result         text        NOT NULL,      -- applied | rejected:<reason> | ignored
    amount         bigint,
    settled_after  bigint      NOT NULL,
    held_after     bigint      NOT NULL,
    reserve_after  bigint      NOT NULL,
    PRIMARY KEY (merchant_id, seq)
);
CREATE INDEX IF NOT EXISTS ledger_entries_asof
    ON ledger_entries (merchant_id, event_time DESC, seq DESC)
    INCLUDE (settled_after, held_after, reserve_after);

CREATE TABLE IF NOT EXISTS auths (
    event_id     text PRIMARY KEY,
    merchant_id  text    NOT NULL,
    amount       bigint  NOT NULL,
    open         boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS captures (
    event_id     text PRIMARY KEY,
    merchant_id  text   NOT NULL,
    amount       bigint NOT NULL,
    refunded     bigint NOT NULL DEFAULT 0
);

-- Payout state machine (see payouts.py):
--   rejected                                        (insufficient funds; terminal)
--   pending -> sending -> paid | failed             (definitive bank answers)
--   sending -> unknown -> (search) -> paid | pending (ambiguous answer; never re-POSTed blindly)
CREATE TABLE IF NOT EXISTS payouts (
    payout_id          text PRIMARY KEY,         -- = the PAYOUT_REQUEST event_id = bank `reference`
    merchant_id        text   NOT NULL,
    account            text,
    currency           char(3),
    amount             bigint NOT NULL,
    status             text   NOT NULL,          -- pending | sending | unknown | paid | failed | rejected
    reason             text,
    bank_payout_id     text,
    key_gen            int    NOT NULL DEFAULT 0, -- idempotency key = payout_id || '#' || key_gen
    attempts           int    NOT NULL DEFAULT 0,
    attempt_started_at timestamptz,               -- written BEFORE each POST (write-ahead intent)
    next_attempt_at    timestamptz NOT NULL DEFAULT now(),
    lease_owner        text,
    lease_until        timestamptz,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payouts_due ON payouts (next_attempt_at)
    WHERE status IN ('pending', 'sending', 'unknown');

-- Every HTTP exchange with the bank, for audit and the "bank error classes" view.
CREATE TABLE IF NOT EXISTS payout_attempts (
    id           bigserial PRIMARY KEY,
    payout_id    text        NOT NULL,
    kind         text        NOT NULL,           -- post | search
    idem_key     text,
    outcome      text        NOT NULL,           -- executed | replayed | account_closed | rate_limited | ...
    http_status  int,
    executor     text        NOT NULL,
    at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payout_attempts_payout ON payout_attempts (payout_id);

-- Session-reader leadership. epoch is the fencing token: strictly increasing on every takeover;
-- every renewal is a compare-and-set on (holder, epoch).
CREATE TABLE IF NOT EXISTS reader_lease (
    name           text PRIMARY KEY,
    holder         text,
    epoch          bigint NOT NULL DEFAULT 0,
    expires_at     timestamptz NOT NULL DEFAULT 'epoch',
    since          timestamptz,
    session_epoch  bigint                         -- the gateway's session_epoch of the current session
);
INSERT INTO reader_lease (name) VALUES ('bosp') ON CONFLICT DO NOTHING;

-- Broker partitions owned by ledger consumers. stream_seq is the consumer offset, committed in the
-- same transaction as the ledger effects (so the broker's own ack state is never trusted).
CREATE TABLE IF NOT EXISTS partitions (
    partition    int PRIMARY KEY,
    owner        text,
    owner_epoch  bigint NOT NULL DEFAULT 0,
    lease_until  timestamptz NOT NULL DEFAULT 'epoch',
    stream_seq   bigint NOT NULL DEFAULT 0,
    lag          bigint NOT NULL DEFAULT 0,
    applied      bigint NOT NULL DEFAULT 0,
    dup_skipped  bigint NOT NULL DEFAULT 0,
    gap_dropped  bigint NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS consumer_members (
    member_id  text PRIMARY KEY,
    heartbeat  timestamptz NOT NULL
);
