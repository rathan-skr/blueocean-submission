# AGENTS.md: briefing for AI coding assistants

Read this before changing anything. Detail is in `docs/CODEBASE.md` (architecture, code map, data model, rules), `docs/HOW-TO.md` (change recipes) and `docs/TESTING.md`.

## What this is
A payments settlement pipeline in Python 3.12: BOSP gateway → `settle.reader` → NATS JetStream → `settle.consumer` → PostgreSQL ledger → `settle.payouts` → bank. `settle.api` serves reads. The external world is simulated by `harness/`.

## Commands
```bash
docker compose up --build -d                          # run everything (harness included)
docker compose --profile test run --rm tests          # tests: expect "45 passed, 7 xfailed"
docker compose run --rm verify                        # end-to-end check after the gateway finishes: expect RESULT: PASS
python scripts/faults.py all                          # fault injection (host)
docker compose logs -f reader consumer payouts        # JSON logs
```

## File map (one line each)
- `settle/config.py`: all settings (environment variables).
- `settle/schema.sql`: tables. Idempotent: only `CREATE … IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`.
- `settle/bosp.py`: pure protocol logic: frame decoder, `ChannelSequencer`, `CreditLedger`.
- `settle/reader.py`: gateway session, leader `Lease`, publish to NATS, credit on PubAck.
- `settle/ledger.py`: `apply_event` (money rules, pure) and `apply_batch` (exactly-once apply).
- `settle/consumer.py`: partition ownership and the one-transaction-per-batch apply.
- `settle/payouts.py`: payout executor (claim, write-ahead, send, reconcile by search).
- `settle/bankapi.py`: bank HTTP client and `classify_post` (the outcome matrix).
- `settle/limiter.py`: Redis-shared rate limit, per-account slots, OAuth token (all fail open).
- `settle/api.py`: HTTP API, `/v1/ops/status`, `/metrics`.
- `settle/partition.py`, `broker.py`, `db.py`, `log.py`: small helpers.
- `tests/`: tests. `tests/incumbent/`: Part C reproducers.
- `harness/`, `incumbent/`: **given by the assignment, read-only.**

## Never do
1. **Never** edit anything in `harness/` or `incumbent/`.
2. **Never** import `ledger_events` from `settle/` (allowed in `tests/` only). **Never** call the bank's `/_admin` from `settle/`.
3. **Never** apply a ledger frame except through `ledger.apply_batch`, whose guard is `seq == last_seq + 1` under a merchant row lock, with `last_seq` updated in the same transaction.
4. **Never** store or advance the consumer offset (`partitions.stream_seq`) outside the apply transaction. Never use NATS acks for it.
5. **Never** set `Nats-Msg-Id` / broker dedup.
6. **Never** return gateway credit before the NATS PubAck. Never put blocking DB or Redis calls in the reader's event loop.
7. **Never** POST a payout whose status is `sending` or `unknown`. It must go through `Executor.reconcile` (wait `RECONCILE_AFTER_S`, then search). Never shorten the reconcile timings without understanding DESIGN.md §7.
8. **Never** update a payout row without `WHERE lease_owner = me`.
9. **Never** make correctness depend on Redis. New Redis code must fail open.
10. **Never** use floats for money (use `bigint` minor units), or wall-clock time inside `apply_event` (use `event_time`).

## Before you say "done"
- [ ] `docker compose --profile test run --rm tests` passes (all `[fixed]` pass; all `[incumbent]` xfail).
- [ ] A new rule or behaviour has a test. A ledger rule is compared with the reference if the harness knows it.
- [ ] `schema.sql` is still idempotent (it runs on every start).
- [ ] For changes to the reader, consumer or payouts: a full run plus `docker compose run --rm verify` gives `RESULT: PASS`, ideally with `scripts/faults.py all` too.
- [ ] `docs/CODEBASE.md` is updated if you changed the data model, a rule in its section 5, or a setting.
