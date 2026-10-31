# DESIGN.md: settlement stream at production scale

**Numbers used throughout** come from ASSIGNMENT §2. Two of them disagree, so I designed for both.

- **Rate versus volume.** "450 M events/month" averages **174 events/s**, but "steady 4,000/s" would be **10.4 B/month**. I sized **throughput** for the stated rates: 4 k/s steady and 30 k/s peak for hours. I sized **storage** for the stated volume: 450 M/month, which over 84 months is **37.8 B rows**. The brief's "≈3 B rows" is 6–7 months of that. Every mechanism below is per-merchant and logarithmic, so it holds at either size.
- **Bank rate limit.** ASSIGNMENT says 50 req/s, `BANK-API.md` says 20 POST/s. Payout maths is given for both.

**Measured locally** on a 2-core sandbox with real PostgreSQL 16, NATS 2.10 and Redis 7, running the default harness and the 2 M-frame run:

| What | Value |
|---|---|
| `ledger_entries` heap | 112 B/event |
| `as_of` covering index | 115 B/event |
| Primary key | 31 B/event |
| WAL | ≈0.9 KB per applied frame (409 MB over 455 k frames in the 2 M run), including the merchant, auth and capture updates and the full-page images (see §5) |
| `as_of` latency over HTTP | p99 4 ms |

---

## 1. Delivery semantics, hop by hop

| Hop | Semantics | Mechanism, and **when** it is written relative to the effect |
|---|---|---|
| **gateway → reader** | At-least-once on the wire. Reordered after loss: go-back-N. | A per-channel `ChannelSequencer` accepts **only `seq = expected`**. Below that is a duplicate, whatever `poss_dup` says. Above that is a gap: send `RESEND from_seq=expected`, re-sent after 2 s if unanswered (lost resend), and drop the frames beyond the gap. A channel silent for 4 s gets a tail-loss probe (`RESEND expected`), which is harmless if nothing was lost. Session **resume = `merchants.last_seq`**, written in the apply transaction (hop 4), so the checkpoint can never be ahead of the effect. |
| **reader → broker** | At-least-once. **Duplicates possible**, loss impossible. | Each in-order frame is published raw to `ledger.p.<crc32(merchant)%P>` with a reply inbox, and the JetStream **PubAck** (file store, R3 in production) is the durability point. The frame's **credit is returned to the gateway only after its PubAck**. A missing or negative PubAck after 5 s kills the session, which then resumes from `last_seq`. So nothing is ever acknowledged upstream that the broker doesn't hold, and anything not acknowledged is resent. Duplicates come from resuming at `last_seq`, which is earlier than published, and from a zombie leader. **No `Nats-Msg-Id` dedup, deliberately:** the broker's 2-minute dedup window would drop a legitimate re-publish of a frame that a consumer had discarded as out-of-order, leaving a permanent hole. |
| **broker → consumer** | At-least-once. | Ephemeral pull consumers, `AckPolicy.NONE`, started at **`partitions.stream_seq + 1`**. The broker's ack state is never used. The offset is written **in the same transaction** as the effects (next row), so a crash rolls back both and the batch is re-read. |
| **consumer → PostgreSQL** | **Effectively-once.** | The transaction is: (1) `SELECT … FROM partitions WHERE owner=me AND owner_epoch=e FOR UPDATE`, the fence; (2) `SELECT … FROM merchants … FOR UPDATE`; (3) apply each frame **iff `seq = last_seq + 1`**, where `seq ≤ last_seq` is a no-op and `seq > last_seq+1` is dropped and will be re-published; (4) effects, then `last_seq`, then `stream_seq`, then COMMIT. The backstop is `PRIMARY KEY (merchant_id, seq)` on `ledger_entries`. |
| **PostgreSQL → executor** | At-least-once claim. | The payout row is created **in the PAYOUT_REQUEST's apply transaction** (status `pending`, with account and currency snapshotted). Executors claim with `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED)`, which sets a 60 s lease. Every later update is fenced `WHERE lease_owner = me`. |
| **executor → bank** | **Effectively-once**, under the assumptions in §7. | Before each POST, `status='sending', attempt_started_at=now()` is **committed** (write-ahead). The POST uses a stable key `payout_id#key_gen`. A definitive "no" (401, 429, 409 inflight, 503, connect error) returns it to `pending`. A definitive yes (201, or 200 replayed) makes it `paid`. `422 account_closed` makes it `failed`. **Anything that might have executed** (502, timeout, 409 in_progress, an unexpected answer, or a `sending` row whose executor died) becomes `unknown`. It is resolved only by `GET /v1/payouts?reference=`, after `attempt_start + timeout + commit grace + search lag` (8 + 5 + 3 + 2 = 18 s). If the search finds it, the payout is `paid`. If it finds nothing, the payout is resent with `key_gen+1`. |

## 2. Fencing: the 45-second stall

**Tokens.**

- The reader lease is a row in PostgreSQL. `epoch` increments on every takeover, and renewal is the CAS `UPDATE … WHERE holder=me AND epoch=e`. TTL 8 s, renewed every 2 s.
- The **real fencing token on the data path is the channel sequence number** itself, checked by the ledger (`seq = last_seq+1`, under a row lock). Channel content is a pure function of `(channel, seq)`, so whatever a zombie publishes is byte-identical to what the new leader publishes.

| t | Old leader A (holds lease epoch 7 and gateway session 501) | Standby B | What stops corruption |
|---|---|---|---|
| 0 | `SIGSTOP`. A holds frames it has received but not published, and open publishes awaiting PubAck. | Polls the lease every 1 s. | n/a |
| 0–7 | Frozen. The gateway keeps A's TCP session up until 2.5 × hb, then logs it out (`heartbeat_timeout`). | n/a | n/a |
| ≈8 | n/a | `UPDATE reader_lease … WHERE expires_at < now()` succeeds and becomes **epoch 8**. Reads `last_seq` and logs on as session 502. | **Gateway:** a new logon supersedes A's session (`LOGOUT superseded`), so A can never *receive* again. |
| 8–45 | Frozen. | Publishes from `last_seq+1`, which re-publishes A's unacknowledged tail. | n/a |
| 45 | `SIGCONT`. A's event loop runs again. Before publishing **any** frame it checks its **local monotonic deadline** (last successful renewal + TTL − 1 s). That deadline passed 38 s ago, so A raises `LeaseLost`, closes the socket and goes back to standby. Its next renewal CAS also fails (epoch 8 ≠ 7). | Unaffected. | **Reader self-fence.** The deadline is measured from *before* each renewal request, so A always stops ≥ 1 s before B can acquire. A pause is caught on wake. |
| 45+ | Residual: a PubAck for a publish A sent *before* the pause can arrive and return credit on a dead socket (ignored). A frame that left A in the instant between its deadline check and the socket write lands in the stream. | n/a | **Ledger:** that frame is a duplicate of `(channel, seq)` and `seq ≤ last_seq` makes it a no-op. If it arrives *ahead* of the stream, `seq > last_seq+1` drops it, and B's stream fills the hole. |

**Observed in the 2 M-frame fault run.** All times are in seconds after `SIGSTOP` (t = 0, at 04:52:50 UTC):

- At **t ≈ 9**, the standby logged `became leader` with lease epoch 3, and its session was up 20 ms later.
- At **t = 45**, `SIGCONT`. The old leader logged `session ended: LeaseLost('local lease deadline passed')`, followed by `lease lost` with epoch 2, **within 1 s of waking**.

For comparison, after the earlier `kill -9` the standby held the session **7.3 s** later.

**Consumers use the same pattern.** `partitions.owner_epoch` is checked with `FOR UPDATE` inside every apply transaction. `idle_in_transaction_session_timeout=15 s` stops a frozen owner holding the row lock. A paused consumer wakes and its transaction finds `owner_epoch` changed: no rows, abort, partition dropped.

**Why a TTL lock alone is insufficient.**

- The holder cannot know it has lost the lock between *checking* it and *acting* on it. A pause (GC, `SIGSTOP`, VM steal, a swap storm) can land after the check.
- A Redis TTL lock also disappears entirely on a non-durable Redis restart. That is incumbent defect #27: both readers "session up" in the same minute.
- Safety needs a **monotonic token checked by the resource being written**. Here that is the ledger's per-channel sequence and the partition `owner_epoch` inside the apply transaction. The lease only provides liveness, meaning that usually only one reader works.

## 3. Backpressure: 30 k/s in, broker slows to 10 k/s for 10 minutes

The **deficit is 20 k frames/s × 600 s = 12 M frames**. At about 450 B per frame on the wire that is about 5.4 GB.

**Where it lives: at the acquirer gateway.** It holds 72 h of replay history, and 12 M frames is about 7 minutes of the 30 k/s peak.

- Credit is returned only on PubAck. When the broker slows to 10 k/s, PubAcks slow to 10 k/s.
- Each channel's outstanding credit therefore drains to 0, and the gateway stops sending that channel.
- Our processes **never** buffer the deficit.

**What bounds memory in each process.**

- **Reader:** unacknowledged publishes ≤ Σ outstanding credit. With a fixed window of 1000 × 200 channels that is ≤ 200 k frames, about 90 MB. For **60 k channels** a fixed window doesn't work (60 M frames). Production uses a **credit budget** instead:
  - a global cap B, for example 100 k frames (about 45 MB) per session;
  - a floor of 8 per channel, which is 480 k for 60 k channels and itself exceeds B, so idle channels get 1;
  - the rest distributed in proportion to each channel's EWMA rate, re-granted as PubAcks return.
  - Hot channels then get thousands and idle channels get 1. The tail-loss probe still works because it is a RESEND, not credit.
- **Broker:** a 10-minute backlog *inside* JetStream (if the DB, not the broker, were slow) is 12 M × 450 B ≈ 5.4 GB on disk. Limits retention: `max_age` 72 h, plus `max_bytes` sized to about 6 h of peak (≈ 290 GB/replica). Exceeding it makes JetStream **reject publishes** (`discard: new`), which causes PubAck errors, then session teardown, so the backlog goes back to the gateway rather than being dropped.
- **Consumers:** `fetch(1000)` per partition; with 256 partitions that is ≤ 256 k frames in memory across the fleet.

**Recovery.** A drain needs headroom. With a consumer capacity of 40 k/s:

- while the peak continues, 12 M are drained at 10 k/s of surplus, in about **20 min**;
- at steady 4 k/s, at 36 k/s of surplus, in about **5.5 min**.

**Credit participates by turning a downstream slowdown into upstream non-sending**, per channel. Hot channels saturate first, so the slowdown is fair across merchants.

## 4. Partitioning and skew

**Key and count.**

- The key is `merchant_id`, mapped `crc32(merchant) % P`, so all of a merchant's frames share one JetStream subject and keep their order.
- P = **256** in production (16 in the assignment). The batched Python applier measured about 2–3 k events/s per partition owner. 30 k/s ÷ 2.5 k = 12 busy partitions minimum. With Zipf skew piling hot merchants together, and room for 4× scale-out, 256 subjects is cheap in JetStream and keeps the largest bucket small.
- Partitions are owned through PostgreSQL leases, so changing ownership is a row update, not a broker rebalance.

**Skew.** With Zipf exponent ≈ 1 across 60 k merchants, H₆₀₀₀₀ ≈ 11.6, so the **top merchant carries about 8.6 %**: about 2.6 k/s at peak. The top 1 % (600 merchants) carry 40 %, about 12 k/s.

**One merchant faster than a partition or worker.**

- Per-merchant order is a hard requirement: payout funds checks depend on everything before them. So one merchant is always applied by one transaction stream.
- Its throughput comes from **batching**, not parallelism. A 1000-event batch for a single merchant is one locked row, in-memory rule application, and one COPY. That is about 3–4 transactions/s to reach 2.6 k/s, comfortably inside one owner.
- If a hot merchant's partition-mates hold it back, a **placement override table** (`merchant → dedicated partition`) isolates it. The move is fenced by sequence, with no pause needed:
  1. The reader starts publishing the merchant's frames from `last_seq+1` to the new subject.
  2. The new owner drops anything with `seq > last_seq+1` until the old partition's tail has been applied. The ledger guard makes the overlap safe in either order.

**Beyond one 25 k frames/s session (16 `client_id`s).**

- Channels map to **session slots**: `slot = f(channel)`, initially hash-based and then bin-packed by observed rate, so each slot carries ≤ 15 k/s.
- Each `client_id` is a leased slot in PostgreSQL: `session_slots(slot, client_id, holder, epoch, channel_map_version)`. Reader replicas acquire slots exactly as consumers acquire partitions, and each slot's leader logs on subscribing **only to the channels the map assigns that slot**.
- **Reassigning a channel** bumps the map version and is two-phase:
  1. The **old** slot re-logs on without the channel. Its next `LOGON` subset excludes it, and only after that does the map mark the channel free.
  2. The **new** slot then re-logs on with the channel, resuming from `last_seq`.
- Two sessions briefly owning a channel would still be *safe* (the ledger guard), but the protocol prevents it so we don't double gateway load.

## 5. PostgreSQL at 3 B (and 38 B) rows

**Schema.**

| Table | Notes |
|---|---|
| `merchants` | 60 k rows. Running `settled`, `held`, `reserve`, sharded `fees`, `last_seq`. `fillfactor 70`. |
| `ledger_entries` | Append-only, one row per DATA event, carrying the **balance after it** (`settled_after`, `held_after`, `reserve_after`). |
| `auths`, `captures` | Keyed by event_id, scoped to the merchant. |
| `payouts`, `payout_attempts` | Payout state and bank audit. |
| `reader_lease`, `partitions`, `consumer_members` | Coordination. |

**Partitioning of `ledger_entries`.** Use `RANGE(event_time)`, **monthly**: 84 live partitions and a 7-year window. Expiry is `DROP PARTITION`. Old months are frozen once (no vacuum churn) and can sit on cheaper tablespace. Add `merchant_month_close(merchant_id, month, settled, held, reserve)`, written when a month is closed.

**The exact `as_of` index.**

```sql
CREATE INDEX ON ledger_entries (merchant_id, event_time DESC, seq DESC)
  INCLUDE (settled_after, held_after, reserve_after);
-- query: SELECT … WHERE merchant_id=$1 AND event_time <= $2 ORDER BY event_time DESC, seq DESC LIMIT 1
```

- **How it reads.** It is an **index-only scan of one leaf entry** in as_of's month partition. If the merchant has no event in that month, it is one probe of `merchant_month_close` for the latest month before as_of.
- **Cost.** At most 2–3 B-tree descents of depth ≤ 5, a few ms even cold. That is **< 200 ms at 3 B or 38 B rows**, because cost depends on depth, not table size.
- **Exactness.** The value is exact, not approximate: the row *is* the balance after the last event ≤ as_of.
- **Visibility map.** Index-only needs it set. Insert-driven autovacuum (PG13+) does that.
- **Write cost.** One extra B-tree insert per event: 115 B/event measured (it is fat because inserts land at random merchant positions, so leaves split to about 70 % full), plus its WAL. That is about **40 % of the per-event write volume**. The PK `(merchant_id, seq)` adds 31 B. 450 M/month × 260 B ≈ **117 GB/month**, about 9.8 TB over 7 years, or about 0.8 TB at "3 B rows".

**Update-heavy rows.**

- **No hot rows.** Platform fees live in a per-merchant `fees` column (sharded counter), and `GET /platform/balances` is `SUM … GROUP BY currency` over 60 k rows (about 10 ms; cache it if needed). There is no global row.
- **`merchants` is updated once per batch per merchant, not per event.** At 30 k/s with 1000-frame batches that is about 30 transactions/s, each touching ≤ a few hundred merchant rows: about 5–10 k row updates/s.
  - Only the PK is indexed, so every update is **HOT**: no index churn, and dead versions are pruned in-page.
  - `fillfactor 70` leaves room for that.
  - Per-table autovacuum: `vacuum_scale_factor=0`, `threshold=5000`, `cost_delay=0`.
  - Bloat stays at about 1–2× the 60 k-row table, which is a few MB.
- **Contention.** One transaction per partition at a time, so different partitions never block each other. `auths` and `captures` take one HOT update each (close or refund).

**WAL volume.**

- Measured: ≈ **0.9 KB WAL per applied frame** (409 MB over 455 k frames) (heap and two index inserts, the amortized merchant/auth/capture updates, and full-page images with `wal_compression=on`, `checkpoint_timeout=15min`).
- At 450 M/month (15 M/day) that is about **14 GB/day**.
- At the stated **steady 4 k/s** it is about **310 GB/day**, and **27 MB/s at the 30 k/s peak**. That needs NVMe WAL, `max_wal_size` ≥ 64 GB, and replication sized for it.

**`synchronous_commit = on`, `fsync = on`.** This is more interesting than "money, so durable".

- The **ledger alone could survive** `synchronous_commit=off`. Effects, `last_seq` and the broker offset commit atomically, so a lost tail rolls them back together and is re-applied from the broker.
- **Payouts cannot.** With `off`, a transaction is *visible* before it is *durable*. An executor could claim and POST a payout whose row then vanishes in a crash. Replay recreates it as a fresh `pending` with key `#0`, and the executor would POST it again. That is a double payment once the bank's key TTL has passed.
- So commits stay synchronous. Batching gives about 30–60 fsyncs/s, which is trivially cheap. In production add a **synchronous standby** (`synchronous_standby_names = 'ANY 1 (…)'`) so failover loses nothing (see §8).

**Connection pooling: PgBouncer in transaction mode** for the API and consumers. Every unit of work is one transaction. That mode **forbids**:

- session-level state: `SET`, session `pg_advisory_lock`, temporary tables, `WITH HOLD` cursors;
- **LISTEN/NOTIFY**;
- server-side prepared statements, unless PgBouncer ≥ 1.21 with `max_prepared_statements`. Otherwise psycopg must use `prepare_threshold=None`.

Our code respects that, with two exceptions:

- The executor's `LISTEN payouts` connects **directly** (polling is the fallback).
- The consumer's `idle_in_transaction_session_timeout` / `statement_timeout` move to `ALTER ROLE ledger_consumer SET …`.

Incumbent defects #32 and #33 are exactly these PgBouncer violations.

## 6. Isolation levels

| Transaction | Level | Why |
|---|---|---|
| Ledger batch apply | **READ COMMITTED + `SELECT … FOR UPDATE`** on the partition row and on the merchant rows | Locks, not snapshots, give the serialization. At RC a `FOR UPDATE` that waits re-reads the **latest committed** row version, so a blocked duplicate sees the advanced `last_seq` and skips. Stronger levels would only add serialization-failure retries. |
| Partition acquire/renew, reader lease acquire/renew | RC, a single `UPDATE … WHERE <CAS> RETURNING` | One statement, atomic compare-and-set on `owner_epoch` / `epoch`. |
| Payout claim | RC, `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED)` | Non-blocking work distribution. |
| Payout transitions | RC, a single `UPDATE … WHERE lease_owner = me` | Fenced CAS. |
| API reads | RC, autocommit, one statement each | One statement is one snapshot, which is enough for a single balance or as_of probe. |

**One anomaly at READ COMMITTED: a lost update on the merchant row**, if `FOR UPDATE` were removed from `SELECT … FROM merchants` in `apply_batch`. This bites during a hot-merchant move (§4), when a merchant's frames briefly exist on two subjects and two partition owners apply concurrently.

```
T1 (partition 7)                         T2 (partition 200, after the move)
SELECT M  -> last_seq=100, settled=10000
                                         SELECT M  -> last_seq=100, settled=10000   (no lock: not blocked)
apply 101 PAYOUT 8000 -> accepted
UPDATE M SET settled=2000, last_seq=101
COMMIT
                                         apply 101 (same frame, other subject) -> accepted again
                                         apply 102 PAYOUT 8000 -> accepted (stale 10000-8000 >= 8000)
                                         UPDATE M SET settled=2000, last_seq=102    <- lost update
                                         COMMIT   => two payouts executed against 10000
```

- **With the lock,** T2 blocks on the row, re-reads `last_seq=101` and `settled=2000`, skips 101 as a duplicate, and rejects 102 for insufficient funds.
- **Today** the `PRIMARY KEY (merchant_id, seq)` would *also* abort T2 (its insert of `(M,101)` conflicts), so the lock and the constraint are two independent guards. Remove both and this interleaving overdraws: the incumbent's defect #12, reached by a different route.

## 7. Payout maths and the ambiguity protocol

**Month-end: 1.5 M payouts in 6 h** is an arrival rate of 69.4/s.

| Bank limit | Capacity in 6 h | Backlog at the end of the 6 h | Drain time (POSTs only) |
|---|---|---|---|
| 50 req/s (brief) | 1.08 M | **420 k** | 1.5 M/50 = **8.3 h** |
| 20 req/s (BANK-API.md) | 432 k | **1.07 M** | 1.5 M/20 = **20.8 h** |

**Overheads on top.**

- About 3 % fail before commit (503), resent at once.
- About 5 % ambiguous: 502 or hang. They wait at least 18 s, then spend a *search*, which has its own 10/s quota. 75 k searches take 2.1 h of search capacity, in parallel with POSTs.
- One 20-minute maintenance window costs 60 k slots at 50/s, about +20 min.
- 429s cost nothing (no money moved).

The realistic finish is about **9 h at 50/s** and about **22 h at 20/s**. **The 6-hour target is unattainable at either limit.** I'd raise it with the bank: a higher limit or a batch file.

**Who waits longest.**

- Claims run in FIFO order of `next_attempt_at`, so the **last requests of the window** wait about 2.3 h at 50/s after the window closes.
- The **per-account limit** (2 in flight × about 0.9 s latency, so about 2.2/s per account) bounds a merchant with many payouts *to the same account*. 1,000 payouts to one account need ≥ 7.5 min whatever the global rate. That doesn't block others: a claimed payout whose account slot is busy goes back with a 0.5 s delay, and others proceed.
- **Ambiguous payouts wait at least 18 s plus search-queue time.**
- Fairness under backlog can switch to `ORDER BY merchant-round-robin` if a few merchants flood the window.

**State machine (ambiguity protocol).**

```
                 claim (lease 60 s)
   pending ────────────────────────▶ [write-ahead COMMIT: status=sending, attempt_started_at, key_gen]
     ▲   ▲                                   │ POST Idempotency-Key = payout_id#key_gen
     │   │  definitive NO                    ├── 201 / 200 replayed ───────────────▶ paid
     │   └──(401,429,409 inflight,503,──────┤
     │       connect error) +delay           ├── 422 account_closed ──────────────▶ failed (never retried)
     │                                       └── 502 / timeout / 409 in_progress /
     │                                           unexpected / executor died ──────▶ unknown
     │                                                                                  │ wait until now ≥ attempt_started_at
     │  search: 0 rows → key_gen+1                                                       │ + timeout + commit grace + index lag
     └──────────────────────────────────────── SEARCH reference ◀──────────────────────┘
                                                   ├── 1 row  → paid (bank_payout_id)
                                                   └── >1 rows → paid + CRITICAL "double payment detected"
```

**Where a double payment could still happen, and what detects it.**

1. **The bank commits an abandoned request later than `attempt_start + 8 s timeout + 5 s grace`**, for example if it was queued at a load balancer for 13 s and then executed. The search finds nothing, so we resend. *Mitigation:* the grace is configurable, and it only adds latency to ambiguous payouts.
2. **The search index lags more than the contracted 3 s.**
3. **An executor paused in the microseconds between its local lease-deadline check and the socket write,** for longer than the remaining lease (≥ 45 s), while another executor resolved and resent the payout.
4. **A database failover to an async replica that loses a `sending` write-ahead after the POST left** (see §8, which forces a search for recent payouts).

**Detection.**

- **Inline:** any search returning more than one row logs CRITICAL and marks `reason=double_payment_detected`.
- **Daily reconciliation** (not built): fetch the bank statement, or search each payout paid in the last 24 h (40 k/day at 8/s is about 1.4 h). Assert exactly one execution per reference, the right amount and account, and no unknown references.
- **Metric:** `settle_bank_responses_total{outcome}` with alerting on ambiguous-rate spikes.

## 8. Failure modes

| Failure | What happens | Self-heals | Pages a human |
|---|---|---|---|
| **Redis lost** (wiped or down) | Rate-limit windows, per-account slots and the shared token vanish. Every primitive **fails open**, and the bank's own limits answer 429 or 409 `too_many_inflight`, both definitive "not executed", so they are retried. The token is re-minted, one per 5 s, behind the lock. Reader status in `/ops` is missing for about 2 s. **No correctness path touches Redis.** | Yes, in seconds. | Only if Redis is down for more than 5 min (executors fall back to per-process tokens, which thrash). |
| **PostgreSQL failover to an async replica** | The last k ms of commits are lost. **Ledger:** effects, `last_seq` and `stream_seq` roll back *together*, so consumers re-read from the older offset (the broker holds 72 h) and re-apply. **Session checkpoint:** it *is* `last_seq`, so the reader resumes earlier and the gateway replays. Both self-heal. **Payouts are the danger:** a lost `sending` or `paid` transition for a POST that did happen. | Ledger and session: yes. | **Always.** Detection is a timeline change (`pg_control_checkpoint().timeline_id`) or `pg_is_in_recovery()` flip, seen by every service on reconnect. Repair is automatic but alarmed: mark every non-terminal payout updated in the last (replication lag + 5 min) as `unknown`, so each goes through **search before any POST**. Then recompute fee totals and spot-check balances against `ledger_entries`. Prevention: a synchronous standby. |
| **Broker partition-leader loss** (JetStream R3 stream leader election, about 2–5 s) | Publishes in flight fail or time out. The reader's PubAck timeout (5 s) tears the session down, re-logs on and resumes from `last_seq`, and the duplicates are dropped by the guard. Consumers' fetches fail, and they re-subscribe from their DB offset. | Yes, in about 10 s. Backlog builds at the gateway (credit). | If the stream is unavailable for more than 1 min. |
| **Gateway checkpoint older than the 72 h replay window** | Resume asks for sequence numbers the gateway no longer holds, and the gap can never be filled from BOSP. **We never skip a sequence number.** | No. | Early warning when any channel's `merchants.updated_at` is older than 24 h while its slot is live. At 72 h it is P1: backfill the missing range from the acquirer's out-of-band export, injected as frames, then resume. |
| **Poison event** (malformed frame, unexpected type shape, constraint violation) | The batch transaction aborts, so the partition offset doesn't move and the partition retries. That shows as `settle_broker_partition_lag` growing. A merchant's later events depend on it, so it is **never skipped silently**. Designed but deferred: bisect the batch to find the frame, record it in `poison_events`, **park only that merchant** (its frames are dropped as gaps until released), and let the rest of the partition flow. | No (by design). | Yes, immediately: partition lag above threshold, or a parked merchant. |

## 9. Three tradeoffs

1. **Python and asyncio for the reader and appliers.** They are readable and easy to change live in the debrief, and the batched applier reaches about 2–3 k events/s per partition owner. The cost is CPU: one reader process handles about 10–15 k frames/s, which is below one session's 25 k/s ceiling. With more time I'd write the reader in Go or Rust, or shard sessions earlier (§4).
2. **Duplicates instead of broker dedup, and resume from *applied* instead of *published*.** Correctness lives in one place, the ledger guard, and the broker can never create a hole. The cost is replay traffic after every reconnect, equal to the consumer lag at that moment, plus consumer CPU to drop it. With more time: within one reader process, resume from `max(applied, durably-published contiguous)`.
3. **Reconcile-by-search instead of same-key retries.** It is safe even when idempotency keys expire, maintenance outlasts them, or a request hangs past its key. The cost is that each ambiguous payout waits at least 18 s and uses search quota (10/s), which matters at month-end. With more time: a same-key retry *only* inside a conservative fraction of the bank's published TTL, while still falling back to search, and a request to the bank to make `reference` unique. That would turn prevention into the bank's job.
