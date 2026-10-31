# CODEBASE.md: how the code works

This guide is for anyone (human or AI) who needs to change this system. Read in order: section 1 is the big picture, section 2 is the code map, section 3 follows one event end to end, section 4 is the data model, section 5 is the rules you must not break, and section 6 is every setting.

---

## 1. The big picture

**What the system does.** A card acquirer (the **gateway**) streams money events for each merchant. We keep each merchant's balance in **PostgreSQL**, and when a merchant asks for a payout we send money through a **bank** API, exactly once.

**Five programs, all in the `settle/` package.** They are run as `python -m settle.<name>`.

| Program | Replicas | Job |
|---|---|---|
| `settle.reader` | 2 (one active) | Holds the single gateway connection. Repairs the stream (gaps, duplicates). Publishes each frame to NATS. |
| `settle.consumer` | 4+ | Reads NATS and applies events to PostgreSQL exactly once, in per-merchant order. |
| `settle.payouts` | 2+ | Sends accepted payouts to the bank exactly once. |
| `settle.api` | 1 | HTTP read API (balances, payouts) plus operator views. |
| (bootstrap) | n/a | Every program creates the schema on start (`db.bootstrap`). |

**Infrastructure and what each one is trusted with:**

| Piece | Trusted with | Not trusted with |
|---|---|---|
| **PostgreSQL** | Everything that matters: balances, the per-channel position (`last_seq`), consumer offsets, leader leases, payout state. **It is the single source of truth.** | n/a |
| **NATS JetStream** | Durable buffering between reader and consumers. One stream `LEDGER`, one subject per partition: `ledger.p.0` … `ledger.p.15`. | Deduplication or offsets: those live in PostgreSQL. |
| **Redis** | Speed-ups only: the shared bank token, rate limits, per-account in-flight slots, reader metrics. | **Anything correctness-related.** It can be wiped at any moment and nothing breaks. |

```
gateway ─TCP/BOSP─▶ reader (leader) ─publish─▶ NATS ledger.p.N ─fetch─▶ consumer ─txn─▶ PostgreSQL
   ▲  CREDIT after NATS ack                                                              │ payouts rows
   └─ resume from merchants.last_seq                                                     ▼
                                                                  payouts executor ─HTTP─▶ bank
                                                       API ◀── reads PostgreSQL ◀── verify.py
```

**The one idea to remember.** Every merchant is a channel with sequence numbers 1, 2, 3… An event with sequence `n` is applied **only if** that merchant's `last_seq` is `n − 1`, and `last_seq` is updated **in the same transaction** as the event's effects. So a duplicate (`n ≤ last_seq`) does nothing, a frame from the future (`n > last_seq + 1`) is dropped and will come again, and nothing is ever applied twice or out of order. Almost every other design choice exists so that this rule stays the only one that matters.

---

## 2. Code map

### `settle/config.py`: settings
Every tunable value, overridable by an environment variable of the same name. See section 6.

### `settle/log.py`: structured logging
`setup(service)` makes every log line a JSON object. Extra fields come from `log.info("msg", extra={...})`, for example `event_id`, `payout_id`, `session_epoch`, `partition`.

### `settle/db.py`: PostgreSQL helpers
| Function | Purpose |
|---|---|
| `connect()` / `aconnect()` | Open a connection, **retrying forever** while PostgreSQL is down (restarts are part of the fault plan). |
| `bootstrap()` | Runs `schema.sql` under an advisory lock (so concurrent starts don't collide) and inserts one `partitions` row per partition. Idempotent. |

### `settle/schema.sql`: the tables
See section 4.

### `settle/bosp.py`: protocol logic with no I/O (pure, unit-tested)
| Name | What it does |
|---|---|
| `encode(obj)` | A dict becomes a 4-byte big-endian length plus JSON. |
| `FrameDecoder.feed(chunk)` | Turns arbitrary TCP chunks into whole frames. Handles splits anywhere, including inside the length prefix. `reset()` drops a partial frame after a disconnect. |
| `ChannelSequencer` | **Per-channel receive state.** `on_frame()` returns `ACCEPT` (seq = expected), `DUP` (below expected) or `GAP` (above expected, plus the `from_seq` to RESEND). `tick(now)` re-sends a RESEND that was lost (after `RESEND_TIMEOUT_S`) and probes a silent channel for tail loss (after `IDLE_PROBE_S`). |
| `CreditLedger` | Credit accounting for one channel. `received()` when a frame arrives. `settle()` when it is durably in NATS or known redundant; returns how much credit to send back (batched in chunks of `CREDIT_REFILL_AT`). Outstanding credit never exceeds the window. |

### `settle/partition.py`
`partition_of(merchant)` is `crc32(merchant) % PARTITIONS`. `subject(p)` gives `ledger.p.<p>`. **All of a merchant's frames go to one subject, which is what keeps their order.**

### `settle/broker.py`: NATS
`connect()` connects with infinite reconnects. `ensure_stream()` creates stream `LEDGER` (file storage, `discard=new`, 72 h). It deliberately sets **no** `Nats-Msg-Id` dedup (rule R6 in section 5).

### `settle/reader.py`: the gateway session
| Name | What it does |
|---|---|
| `Lease` | Leadership row in `reader_lease`. `try_acquire()` takes it if expired and **increments `epoch`**. `renew()` is a compare-and-set on `(holder, epoch)`. `valid()` checks a **local monotonic deadline**, which is how a paused process notices on wake that it must stop. |
| `Session.run(resume)` | One gateway connection: LOGON (with `resume` = applied `last_seq` per channel), read the LOGON_ACK, grant `CREDIT_WINDOW` per channel, then run `_recv_loop` and `_timers`. |
| `Session._handle_bodies` | Per frame: sequenced frames go through `ChannelSequencer`. ACCEPT means publish the raw bytes to NATS with a reply inbox. DUP or GAP means give the credit back now and maybe send RESEND. Also answers `TEST_REQUEST` (echoing `test_req_id`) and handles `LOGOUT`. **Checks `lease.valid()` before every publish.** |
| `Session.on_ack` | A NATS PubAck arrived, so the frame is durable and its credit is returned to the gateway. An error ack sets `failed`, which ends the session. |
| `Session._timers` | Every 0.2 s: server silence > 2 × heartbeat, a lease no longer valid, or a PubAck older than `PUBACK_TIMEOUT_S` closes the session. Also sends sequencer RESENDs and our own heartbeat. |
| `load_resume()` | `SELECT merchant_id, last_seq FROM merchants`: the resume point is **what is applied**. |
| `main()` | Loop: get the lease (or wait), run a session, and on any end reconnect. On `superseded` it gives up leadership. |

### `settle/ledger.py`: the money rules
| Name | What it does |
|---|---|
| `Book`, `Auth`, `Capture` | In-memory state for one merchant, auth or capture. |
| `apply_event(book, ev, auths, captures)` | **The ledger rules from ASSIGNMENT §4**, pure Python. Returns `"applied"`, `"rejected:<reason>"` or `"ignored"`, plus a payout dict for PAYOUT_REQUEST. It matches `harness/ledger_events.apply_event` exactly (tested). References must point at the **same merchant's** auth or capture. |
| `apply_batch(cur, frames)` | **Exactly-once apply** inside the caller's transaction. (1) Lock the merchant rows (`FOR UPDATE`). (2) Load referenced auths and captures. (3) For each frame, apply only if `seq == last_seq + 1` (GAP_FILL advances over its range, NOTICE only advances). (4) Write everything back: `COPY` for entries, auths, captures; `UPDATE … FROM unnest` for changes; `INSERT` payouts plus `pg_notify('payouts')`. Returns counts: applied, duplicates, gaps. |

### `settle/consumer.py`: applying from NATS
| Name | What it does |
|---|---|
| `Consumer.coordinate()` | Every 1 s: heartbeat in `consumer_members`, compute my partitions (live members sorted, round-robin), release ones I no longer own, renew my leases, acquire free or expired ones (which **increments `owner_epoch`**). |
| `Consumer.run_partition(o)` | For one owned partition: an ephemeral NATS pull consumer starting at `stream_seq + 1`, then `fetch(CONSUMER_BATCH)` and `apply()`, looping. On a DB error it re-reads from the committed offset. |
| `Consumer.apply(conn, o, msgs)` | **One transaction per batch.** `SELECT … FROM partitions WHERE owner=me AND owner_epoch=mine FOR UPDATE` is the fence, then `ledger.apply_batch`, then `UPDATE partitions SET stream_seq = <last msg>` (the offset), then COMMIT. If the fence finds nothing, someone else owns the partition: stop. If the stored offset differs from ours, raise `Resync` and re-read from the DB's value. |

### `settle/api.py`: HTTP
| Route | Source |
|---|---|
| `GET /v1/merchants/{id}/balance` | `merchants` row. |
| `GET /v1/merchants/{id}/balance?as_of=` | The last `ledger_entries` row with `event_time ≤ as_of`: one index-only probe. Zeros before the first event. |
| `GET /v1/platform/balances` | `SUM(fees)` per currency over `merchants`. |
| `GET /v1/payouts/{id}` | `payouts` row. Internal `sending`/`unknown` are shown as `pending`. 404 if not applied yet. |
| `GET /healthz`, `/readyz` | Process is up / PostgreSQL reachable. |
| `GET /v1/ops/status` | Operator view: leader, epochs, channels behind, partitions and lag, consumers, payouts by state, bank outcomes. |
| `GET /metrics` | The same in Prometheus format. |

### `settle/bankapi.py`: the bank client
| Name | What it does |
|---|---|
| `classify_post(status, body, headers, exc)` | **The payout outcome matrix** (pure, tested). It answers "could money have moved?" with `PAID`, `FAILED` (account_closed, permanent), `RETRY` (definitely not executed) or `AMBIGUOUS` (maybe executed). |
| `Bank.mint_token()` / `post_payout()` / `search()` | Thin HTTP calls (httpx). `search()` is `GET /v1/payouts?reference=`. |

### `settle/limiter.py`: shared limits in Redis (all fail open)
| Name | What it does |
|---|---|
| `SlidingWindow.acquire()` | Global requests per second, an atomic Lua script on Redis server time. Used for POST (18/s) and search (8/s). |
| `AccountSlots.try_acquire(account)` | At most 2 in-flight payouts per bank account across all executors; slots expire if a holder dies. |
| `TokenCache.get()` / `invalidate(tok)` | The one shared OAuth token. Only the holder of a short Redis lock mints a new one. `invalidate` deletes only if the stored token is still the rejected one. |

### `settle/payouts.py`: the executor
| Name | What it does |
|---|---|
| `Executor.claim(n)` | `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED)`: take up to n due payouts with a 60 s lease. |
| `Executor.work(row)` | `pending` goes to `send()`. `sending` or `unknown` go to `reconcile()` first. |
| `Executor.send(p)` | Account slot, then rate slot, then token, then **write-ahead** (`status='sending'`, `attempt_started_at=now()`, committed), then POST with key `payout_id#key_gen`, then `classify_post`, then the next status. |
| `Executor.reconcile(p)` | Waits until `attempt_started_at + RECONCILE_AFTER_S` (any commit is visible by then), then searches the bank by reference. Found means `paid`. Not found means send again with `key_gen + 1`. |
| `Executor.release(pid, status, delay, …)` | Every status change, **fenced on `lease_owner = me`**. |
| `Executor.listen()` | `LISTEN payouts` to wake up when the consumer inserts payouts (polling every 0.5 s is the fallback). |

---

## 3. One event, end to end

A merchant `M-0042` requests a payout; the gateway sends channel seq 1187.

1. **Gateway → reader.** The frame `{"type":"DATA","channel":"M-0042","seq":1187,"event":{"type":"PAYOUT_REQUEST","amount_minor":125000,…}}` arrives on the TCP socket. `FrameDecoder.feed` cuts it out (`bosp.py`), and `Session._handle_bodies` parses it (`reader.py`).
2. **Sequencing.** `ChannelSequencer.on_frame("DATA", 1187, …)`: `expected` is 1187, so the verdict is ACCEPT and `expected` becomes 1188. (Had 1186 been lost, this would be GAP: send `RESEND from_seq=1186` and drop 1187; it comes again after the resend.)
3. **Fence check.** `lease.valid()`: if this process was paused past its lease, it stops here.
4. **Publish.** The raw frame bytes go to subject `ledger.p.<crc32("M-0042") % 16>` with reply inbox `…<n>`. `pending[n] = ("M-0042", time)`.
5. **PubAck → credit.** NATS stores it durably and replies. `Session.on_ack` calls `credit["M-0042"].settle()`, which may send `CREDIT n=250` to the gateway. The gateway can now send more for this channel.
6. **Consumer fetch.** The consumer that owns that partition (`consumer.py`, `run_partition`) fetches a batch that includes our frame.
7. **One transaction.** `Consumer.apply`:
   - fence: `SELECT stream_seq FROM partitions WHERE partition=… AND owner=me AND owner_epoch=e FOR UPDATE`;
   - `ledger.apply_batch`: lock `merchants` row M-0042 (`last_seq` = 1186), so 1187 = 1186 + 1 and the frame is applied;
   - `apply_event`: available = settled − reserve ≥ 125000, so settled −= 125000 and the result is a payout `{status: "pending", account: <bank_account now>, currency, amount}`;
   - writes: `ledger_entries` row (M-0042, 1187, … `settled_after`, `held_after`, `reserve_after`), `payouts` row `pending`, `merchants` (settled, `last_seq` = 1187), `pg_notify('payouts')`, `partitions.stream_seq` = this message's stream sequence;
   - COMMIT.
8. **API.** `GET /v1/merchants/M-0042/balance` now shows `last_seq: 1187`. `GET /v1/payouts/M-0042:1187` shows `pending`.
9. **Executor claims.** NOTIFY wakes `Executor.run`, and `claim()` leases the payout to this executor for 60 s.
10. **Send.** `Executor.send`: take an account slot, a rate slot and a token, then commit the **write-ahead** (`status=sending`, `attempt_started_at=now()`), then `POST /v1/payouts` with `Idempotency-Key: M-0042:1187#0`.
11. **Outcome.**
    - `201`: `paid`, with `bank_payout_id` stored. Done.
    - `502` or a timeout: `unknown`, rechecked after 18 s.
    - `503 maintenance`: `pending` again, retried after `Retry-After`.
12. **Reconcile, only if `unknown`.** After 18 s, `GET /v1/payouts?reference=M-0042:1187`. One row means `paid`; zero rows means send again with key `#1`.
13. **API** shows `paid` (or `failed` for a closed account).

---

## 4. Data model (`settle/schema.sql`)

| Table | Row | Written by | Invariants |
|---|---|---|---|
| `merchants` | One per merchant/channel: `currency`, `reserve`, `bank_account`, `settled`, `held`, `fees`, **`last_seq`** | `ledger.apply_batch` only | `last_seq` and the balances change **only** in the apply transaction, together. `fees` is this merchant's share of platform fees (no global hot row). `fillfactor 70` for HOT updates. |
| `ledger_entries` | One per DATA event: `seq`, `event_time`, `result`, `amount`, **balance after** (`settled_after`, `held_after`, `reserve_after`) | `apply_batch` (COPY) | Append-only. PK `(merchant_id, seq)` backstops double apply. Index `ledger_entries_asof` makes `as_of` one index-only probe. |
| `auths` | One per AUTH: `amount`, `open` | `apply_batch` | Only changed while holding the owning merchant's row lock. |
| `captures` | One per applied CAPTURE: `amount`, `refunded` | `apply_batch` | Same. |
| `payouts` | One per PAYOUT_REQUEST: `account` and `currency` **snapshotted at acceptance**, `amount`, `status`, `key_gen`, `attempt_started_at`, lease | Created by `apply_batch`; then only `payouts.py` | Status flow in section 5, rule R8. Every executor update has `WHERE lease_owner = me`. `attempt_started_at` is committed **before** each POST. |
| `payout_attempts` | One per bank HTTP exchange | `payouts.py` | Audit trail and "bank error classes". |
| `reader_lease` | One row `bosp`: `holder`, **`epoch`**, `expires_at`, `session_epoch` | `reader.Lease` | `epoch` strictly increases on every takeover (the fencing token). |
| `partitions` | One per NATS subject: `owner`, **`owner_epoch`**, `lease_until`, **`stream_seq`** (offset), counters | `consumer.py` | `stream_seq` changes only in the apply transaction. `owner_epoch` increases on every takeover. |
| `consumer_members` | One per live consumer process: `heartbeat` | `consumer.coordinate` | Live = heartbeat within `MEMBER_TTL_S`. |

---

## 5. Rules you must not break

| # | Rule | Where | Why / what breaks if you change it |
|---|---|---|---|
| R1 | Apply a frame **only if `seq == last_seq + 1`**, under `SELECT … FOR UPDATE` on the merchant row, and update `last_seq` in the **same transaction** as the effects. | `ledger.apply_batch` | This is the exactly-once guarantee and the real fence against zombie publishers. Without it, duplicates double-apply (incumbent defect #9). |
| R2 | The consumer offset (`partitions.stream_seq`) is written **in the same transaction** as the effects, after the owner/epoch fence. | `consumer.apply` | Otherwise a crash between the two either skips or re-applies messages. Never use NATS acks for this. |
| R3 | The reader's resume point is **`merchants.last_seq`**, not anything the reader stores. | `reader.load_resume` | It can never be ahead of what is applied, so nothing is skipped. Duplicates are fine (R1). |
| R4 | Return credit to the gateway **only after the NATS PubAck** (or for a frame that is a duplicate or beyond a gap). | `Session.on_ack`, `_handle_bodies` | Credit is our promise "this is safe". Returning it early lets the gateway outrun durability. A missing PubAck must end the session. |
| R5 | Check `lease.valid()` (local monotonic deadline) **before every publish**. | `Session._handle_bodies` | A paused leader stands down on wake. Do not replace it with a database call: the I/O loop must never block. |
| R6 | **Never** set `Nats-Msg-Id` (broker dedup). | `broker.py` | The broker would drop a legitimate re-publish of a frame a consumer had dropped as a gap: a permanent hole. Dedup belongs to R1 only. |
| R7 | Never block the reader's asyncio loop (no sync DB or Redis calls in it). | `reader.py` | Heartbeats would be late and the gateway would disconnect (incumbent defect #29/#37). |
| R8 | Payout statuses: `pending` → `sending` (committed **before** POST) → `paid` / `failed` / `pending` / `unknown`. **Never re-POST an `unknown` or `sending` payout without a search that finds nothing**, and only after `RECONCILE_AFTER_S`. | `payouts.py` | Bank idempotency keys expire (120 s, or 20 s in review) and a request can commit and then hang 25 s. A blind retry is a double payment, a P1 (incumbent defect #1). |
| R9 | `422 account_closed` is `failed`, never retried. The money stays deducted. | `bankapi.classify_post` | Required by the brief, and a retry loop freezes the executor (incumbent defect #31). |
| R10 | Every payout status update is fenced `WHERE lease_owner = me`. | `Executor.release`, `send` | Two executors must never both act on one payout. |
| R11 | Redis is **never** needed for correctness. Every Redis call must fail open or degrade. | `limiter.py`, `reader.publish_metrics` | The fault plan wipes Redis mid-run. |
| R12 | Money is `bigint` minor units everywhere. No floats. | everywhere | Incumbent defect #8. |
| R13 | A ref (`ref` of CAPTURE, REFUND…) must belong to the **same merchant**. Unknown event types are `ignored` but still consume their seq. | `ledger.apply_event` | Matches the reference rules. |
| R14 | `ledger.apply_event` must stay identical to `harness/ledger_events.apply_event`. The pipeline must **never import** `ledger_events` (tests only). Never call the bank's `/_admin`. Never edit `harness/`. | n/a | Disqualifying. |

---

## 6. Configuration (`settle/config.py`)

Every value can be overridden by an environment variable with the same name (set them in `docker-compose.yml`).

| Variable | Default | Used by | Meaning / when to tune |
|---|---|---|---|
| `DATABASE_URL` | `postgresql://ledger@localhost:5432/ledger` | all | PostgreSQL. |
| `NATS_URL` | `nats://localhost:4222` | reader, consumer | NATS. |
| `REDIS_URL` | `redis://localhost:6379/0` | reader, payouts, api | Redis (non-critical). |
| `INSTANCE_ID` | `hostname:pid` | all | Identity in leases and logs. In Docker the hostname is the container id. |
| `STREAM` / `SUBJECT_PREFIX` | `LEDGER` / `ledger.p` | reader, consumer | NATS names. |
| `PARTITIONS` | 16 | reader, consumer, bootstrap | Number of subjects. **Changing it remaps merchants**: see HOW-TO. |
| `STREAM_MAX_AGE_S` | 259200 (72 h) | broker | Stream retention. |
| `GATEWAY_HOST` / `GATEWAY_PORT` / `GATEWAY_CLIENT_ID` / `GATEWAY_SECRET` | `localhost` / 7100 / `bo-ledger` / `bo-dev-secret` | reader | Gateway login. |
| `CREDIT_WINDOW` | 1000 | reader | Credit kept outstanding per channel. ≤ 10000 (gateway limit). Larger means more throughput per hot channel and more in flight. |
| `CREDIT_REFILL_AT` | 250 | reader | Return credit in chunks of at least this many frames. |
| `RESEND_TIMEOUT_S` | 2.0 | reader | A RESEND not answered by then is sent again (lost resend). |
| `IDLE_PROBE_S` | 4.0 | reader | A silent channel gets a RESEND probe (tail loss). |
| `PUBACK_TIMEOUT_S` | 5.0 | reader | An unacknowledged publish older than this ends the session. |
| `READER_LEASE_TTL_S` / `READER_LEASE_RENEW_S` | 8.0 / 2.0 | reader | Failover takes about TTL + 1 s. TTL must exceed renew × 2. |
| `CONSUMER_BATCH` | 1000 | consumer | Max frames per apply transaction. |
| `MEMBER_TTL_S` / `PARTITION_LEASE_S` | 6.0 / 10.0 | consumer | Consumer liveness and partition lease length (worst-case takeover time). |
| `BANK_URL` / `BANK_CLIENT_ID` / `BANK_CLIENT_SECRET` | `http://localhost:7200` / `bo-settlement` / `bo-bank-dev-secret` | payouts | Bank. |
| `BANK_RATE_LIMIT` / `BANK_SEARCH_RATE_LIMIT` | 18 / 8 | payouts | Below the bank's 20 / 10 per second. |
| `BANK_INFLIGHT_PER_ACCOUNT` | 2 | payouts | The bank's per-account limit. |
| `BANK_REQUEST_TIMEOUT_S` | 8.0 | payouts | HTTP timeout. A timeout makes the payout `unknown`. |
| `BANK_COMMIT_GRACE_S` / `BANK_SEARCH_LAG_S` / `BANK_SEARCH_MARGIN_S` | 5 / 3 / 2 | payouts | With the timeout they sum to `RECONCILE_AFTER_S` = 18 s: the time after which a search result is trusted. **Lowering them risks double payments.** |
| `PAYOUT_WORKERS` | 16 | payouts | Concurrent payouts per executor process. |
| `PAYOUT_LEASE_S` | 60 | payouts | Claim lease. Must comfortably exceed the request timeout. |
| `API_PORT` | 8080 | api | HTTP port. |
