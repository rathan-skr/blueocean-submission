# HOW-TO.md: recipes for common changes

Each recipe lists the files to touch, the code shape, and the tests to add. Read `CODEBASE.md` section 5 ("Rules you must not break") first. Every recipe ends with **verify**:

```bash
docker compose --profile test run --rm tests       # unit + integration + reproducers
docker compose down -v && docker compose up --build -d && docker compose run --rm verify   # end to end
```

---

## 1. Add a new ledger event type

Example: `ADJUSTMENT` with `amount_minor` (positive or negative) that changes `settled`.

1. **`settle/ledger.py` → `apply_event`.** Add a branch before the final `return "ignored", None`:
   ```python
   if typ == "ADJUSTMENT":
       book.settled += int(ev["amount_minor"])
       return "applied", None
   ```
   - The function must stay **pure**: no I/O, and no wall-clock time. Use `ev["event_time"]` if you need time.
   - If the event refers to another record, use `own(...)` so it can only touch this merchant's records.
   - If it creates new state (like an auth), add a dataclass and a table, load it in `apply_batch` step 2 and write it in step 4, following `auths`.
2. **Nothing else changes.** Sequencing, exactly-once and offsets are generic. `ledger_entries` records the result automatically.
3. **Tests (`tests/test_ledger_rules.py`).** Add a small test like `test_partial_capture_releases_whole_hold`. If the harness reference (`ledger_events.apply_event`) also learns the type, `test_rules_match_reference_across_channels` covers it.

> Until the reference implements it, `verify.py` will disagree on balances for streams that contain it. That's expected for a rule the harness doesn't know.

---

## 2. Payouts must respect a per-merchant daily limit

This is decided **at apply time, in sequence order**, exactly like the funds check. Never decide it in the executor: the executor runs out of order and retries, so a decision there wouldn't be deterministic.

1. **Where the limit comes from.**
   - A global default: `DAILY_PAYOUT_LIMIT_MINOR` in `config.py`.
   - Or per merchant: a new field on `MERCHANT_CONFIG`, stored in `merchants.daily_limit`.
2. **Schema (`schema.sql`).** It is idempotent, so use `ALTER TABLE … ADD COLUMN IF NOT EXISTS`:
   ```sql
   ALTER TABLE merchants ADD COLUMN IF NOT EXISTS payout_day date;
   ALTER TABLE merchants ADD COLUMN IF NOT EXISTS paid_today bigint NOT NULL DEFAULT 0;
   ```
3. **`ledger.py`.**
   - `Book`: add `payout_day: str | None = None` and `paid_today: int = 0`.
   - `apply_batch` step 1: add the columns to the `SELECT … FOR UPDATE` and to the `Book(...)` construction.
   - `apply_batch` step 4: add them to the `UPDATE merchants … FROM unnest(...)`.
4. **The rule, in `apply_event`, in the `PAYOUT_REQUEST` branch, before the funds check.**
   - The day comes from **`event_time`, not the clock**, so replays give the same answer.
   ```python
   day = ev["event_time"][:10]                       # UTC date of the event
   if book.payout_day != day:
       book.payout_day, book.paid_today = day, 0
   if book.paid_today + amt > config.DAILY_PAYOUT_LIMIT_MINOR:
       return "rejected:daily_limit", {**payout, "status": "rejected", "reason": "daily_limit"}
   ...
   book.settled -= amt
   book.paid_today += amt                            # only on acceptance
   ```
   - Decide whether a payout that later **fails** at the bank (closed account) should give the allowance back. By default it shouldn't: the money stayed deducted.
5. **Tests.**
   - Same day: two payouts that exceed the limit together means the second is rejected.
   - The next day it resets.
   - A rejected payout doesn't consume allowance.
   - Replaying the same frames gives the same results (`test_exactly_once_pg.py` style).

---

## 3. The gateway now sends `CHANNEL_RESET`

**First, pin down the semantics**: ask, or read the new gateway. The likely meaning is "this channel's sequence numbers restart at 1 (a new channel epoch)". Your design depends on it:

1. **Reader (`reader.py` `_handle_bodies`, `bosp.ChannelSequencer`).**
   - Treat `CHANNEL_RESET` as an **ordered event in the stream**, never as local state only. Otherwise the reader and the ledger disagree after a crash.
   - Publish it to the channel's subject like a data frame, then reset the sequencer: `self.seq[ch] = ChannelSequencer(1, …)`.
   - Unknown frame types currently only log a warning: handle the new type explicitly.
2. **Ledger.**
   - Add `merchants.channel_epoch`. When a `CHANNEL_RESET` frame is applied in sequence, do `channel_epoch += 1`, `last_seq = 0`.
   - The `ledger_entries` key becomes `(merchant_id, channel_epoch, seq)`.
   - Every frame must carry the epoch it belongs to, stamped by the reader, so frames from before the reset are recognised as old.
3. **Resume.** `load_resume()` must send the position **within the current epoch**. Check what the new protocol expects in `resume`.
4. **Tests.** Sequencer: frames after a reset are accepted from 1. Ledger: duplicates of pre-reset frames are ignored after the reset. Resume after a crash in the middle of a reset.

> The point to say in the debrief: a reset is a *state change of the channel*, so it must travel **through the same ordered pipe** and be applied **in the same transaction mechanism** as everything else. Otherwise replay breaks.

---

## 4. Change the number of partitions

`PARTITIONS` decides `crc32(merchant) % PARTITIONS`, so changing it **moves merchants to other subjects**. Offsets are per partition, so this needs a short controlled stop:

1. Stop the readers: `docker compose stop reader`.
2. Wait until every partition shows `lag` 0 at `/v1/ops/status`, so everything published is applied.
3. Stop the consumers. Delete the NATS stream (`nats stream rm LEDGER`, or the `delete_stream` API) and `DELETE FROM partitions`.
4. Set `PARTITIONS=N` for **all** services in `docker-compose.yml`, then start everything. Bootstrap creates N partition rows. The readers resume from `last_seq`, so no event is lost: the gateway replays anything after the last applied seq.

Moving a single hot merchant to its own partition without stopping is designed in DESIGN.md §4 but not built.

---

## 5. Add a metric or an ops view

- **From the database** (payouts, partitions, leases): add a query in `api.metrics()` with `g("settle_<name>", value, label=...)`, and/or a field in `api.ops_status()`.
- **From the reader's memory** (counters): increment `metrics["<name>"]` in `reader.py`. `publish_metrics` copies it to Redis key `reader:status` every 2 s, and `api.metrics()` reads it from there (add the name to the list in `metrics()`).
- **Logs:** always pass identifiers in `extra={...}` (`event_id`, `payout_id`, `session_epoch`, `partition`) so log searches work.

---

## 6. Debugging

**Is anything behind?** Look at `GET http://localhost:8080/v1/ops/status`:

| Field | Meaning |
|---|---|
| `channels_behind` | `apply_lag` means received but not yet applied (consumer problem). `receive_gap` means the gateway sent higher seqs we're waiting to fill (reader or resend problem). |
| `broker_partitions[].lag` | Messages waiting in NATS for that partition. Owner `null`, or `lease_live: false` for a long time, means no consumer is taking it. |
| `session` | Who holds the gateway session: `lease_epoch` and `gateway_session_epoch`. |

**A channel is stuck.**
```sql
SELECT merchant_id, last_seq, updated_at FROM merchants ORDER BY updated_at LIMIT 10;   -- least recently advanced
SELECT * FROM partitions ORDER BY lag DESC;                                               -- consumer side
```
- `docker compose logs reader | findstr M-0042` (PowerShell) shows the resend and gap history for that channel.
- A partition whose lag keeps growing while its consumer logs `partition loop error` has a **poison event**. The batch keeps failing and is never skipped (by design). Read the error, fix the code, restart the consumer.

**A payout is stuck.**
```sql
SELECT payout_id, status, attempts, key_gen, reason, attempt_started_at, next_attempt_at, lease_owner
FROM payouts WHERE status IN ('pending','sending','unknown') ORDER BY created_at;
SELECT * FROM payout_attempts WHERE payout_id = 'M-0042:1187' ORDER BY id;               -- every bank exchange
```
- `unknown` waits 18 s, then searches. During a bank maintenance window everything waits for `Retry-After` (30 s).
- **Never** set a payout back to `pending` by hand unless a bank search proves it was not executed.

**Logs** are JSON, one line per event:
```
docker compose logs -f reader consumer payouts
```
