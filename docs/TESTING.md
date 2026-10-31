# TESTING.md

There are three layers. Run the first two on every change, and the third before submitting or after touching the reader, consumer or payouts.

| Layer | Command | Time | Needs |
|---|---|---|---|
| 1. Unit + integration tests | `docker compose --profile test run --rm tests` | about 40 s | postgres, redis, nats containers (started automatically) |
| 2. End to end | `docker compose down -v && docker compose up --build -d`, wait for the gateway to finish (about 9 min), then `docker compose run --rm verify` | about 10 min | the whole stack |
| 3. End to end under faults | see "Fault injection" below | about 15 min | the whole stack plus Python on the host |

**Expected result of layer 1:** `46 passed, 7 xfailed`. Run one file with `docker compose --profile test run --rm tests tests/test_bosp.py -v`.

## What each test file proves

| File | Kind | Proves |
|---|---|---|
| `tests/test_ledger_rules.py` | unit | `ledger.apply_event` gives **identical** results to the harness reference on 40 generated channels × 3000 frames, and every rule branch is hit (over-refund, double capture, capture of an expired auth, config change, unknown type…). Refs to another merchant's auth are rejected. |
| `tests/test_bosp.py` | unit, deterministic | Frame decoder: any TCP split, several frames per chunk, oversize frames, dropping a partial frame on reconnect. Sequencer: in order, duplicates regardless of `poss_dup`, a gap means one RESEND then discard, a **lost resend** is re-issued, GAP_FILL (including overlapping ones), a **tail-loss** idle probe, resume. Credit never exceeds the window and is held until settled. |
| `tests/test_bank_outcomes.py` | unit | **Every row of the BANK-API.md table** is classified PAID, FAILED, RETRY or AMBIGUOUS. A timeout is ambiguous, a connect failure is a definite no, a plain 200 without the replay header is not trusted. |
| `tests/test_limiter.py` | integration (real Redis, 6 OS processes) | The global rate limit holds across processes in every sliding 1 s window. The per-account in-flight limit (≤ 2) holds across processes. The limiter fails open without Redis. |
| `tests/test_exactly_once_pg.py` | integration (real PostgreSQL, 6 OS processes) | Six processes push the **same** frames concurrently, with duplicates, out-of-order frames and random batch sizes. Each frame advances its channel **exactly once** in total, and balances, fees and payouts equal the reference. |
| `tests/test_ui_api.py` | integration (real PostgreSQL, Redis down) | The console's endpoints serve live ledger data, and its invariant checks pass on a correct ledger. |
| `tests/incumbent/test_r1…r7` | Part C reproducers | See below. |

## Reading the Part C reproducers

Each reproducer runs **one property twice**:

- `[incumbent]` runs the old team's code. It is marked `xfail(strict=True, raises=AssertionError)`, so it must **fail on the property's assertion**. An import error or crash does not count. If it ever passed, the suite would fail (strict).
- `[fixed]` runs our code. It must pass.

So `x.` per file means "bug reproduced, fix holds". To see *how* the incumbent fails:
```
docker compose --profile test run --rm tests tests/incumbent --runxfail
```
| Reproducer | Expected incumbent failure |
|---|---|
| R1 double payment | `assert 3 == 1`: the bank executed the payout 3 times |
| R2 outbox skip | only `P-late` sent, `P-early` never |
| R3 split brain | 2 leaders |
| R4 redelivery | settled 19 360 instead of 9 680 |
| R5 write skew | 2 payouts accepted with funds for 1 |
| R6 resend loss | `[1, 2, 4, 5, 5]` instead of `[1, 2, 3, 4, 5]` |
| R7 closed account | no result after 3 s (looping forever) |

R1 and R7 start the **real** `harness/bank.py` on a free port. R2–R5 use real PostgreSQL databases `incumbent_test` and `ledger_test`, which are recreated per test. R3 uses Redis db 15. Your real data in db `ledger` and Redis db 0 is not touched.

## Testing by hand with the console

Open http://localhost:8080/ui. Its **Test** tab runs live invariant checks. During a fault drill, keep the strip in view: the reader's epoch rises on failover, partitions change owner after a consumer is killed, and payouts pass through "unclear" and resolve.

## Fault injection (§5.4)

On the host (needs Python 3.11+ and the Docker CLI), with the stack running:

```
python scripts/faults.py all            # whole schedule, about 2.5 min, 15 s apart
python scripts/faults.py kill-leader    # or one at a time (see the script's help)
docker compose run --rm verify          # after the gateway finishes
```

The script prints what it did and what it observed. For example, `new leader holds the gateway session after 8.4s`.

**Harsher chaos** (5× drops, frequent disconnects, a short bank TTL, frequent maintenance windows):
```
docker compose down -v
$env:GATEWAY_EVENTS_PER_CHANNEL=10000; $env:GATEWAY_RATE=3000          # PowerShell
docker compose -f docker-compose.yml -f docker-compose.review.yml up --build -d
```

**What "good" looks like during a fault run** (`/v1/ops/status`):
- `session.lease_epoch` goes up by one per failover.
- Partition owners change after a consumer kill or scale-up, and `lag` returns to around 0.
- `payouts_by_state` shows `unknown` rising during bank trouble and draining afterwards.
- `bank_outcomes` shows `bad_gateway` / `timeout` resolved by `search` → `found_1` (it executed) or `found_0` (it was resent).

## Adding a test

- **Pure logic** (rules, sequencer, classification): plain pytest, no I/O.
- **Anything with SQL:** call `tests.pg.fresh_db()`, which recreates `ledger_test`, points `settle.config.DATABASE_URL` at it and bootstraps the schema.
- **Anything with Redis:** use `TEST_REDIS_URL` (db 15).
- **Anything with the bank:** `with tests.bankproc.bank(BANK_P_FAIL_AFTER_COMMIT=1.0) as url:`. Count real executions with `tests.bankproc.executions(url, reference)`. Using `/_admin` there is fine, because it is a test.
