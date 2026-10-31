# REVIEW.md: the incumbent (v0.9)

**Scope.** I read all of `incumbent/` against the harness contracts: `PROTOCOL.md`, `BANK-API.md`, `ledger_events.apply_event`, and the gateway and bank source. This document has three parts:

- §1 lists the defects, ranked by severity. Each has a `file:line`, a trigger, the impact and a fix.
- §2 explains how the defects combine into the three incident symptoms.
- §3 lists the executable reproducers, in `tests/incumbent/`. Each runs the same property twice: against the incumbent logic, marked `xfail(strict=True, raises=AssertionError)` so it must genuinely fail on the assertion, and against our implementation, where it must pass.

```
docker compose --profile test run --rm tests -k incumbent     ->  7 passed, 7 xfailed
```

**Severity scale:**

- **S1:** money moved wrongly (paid twice, or never paid).
- **S2:** the ledger is wrong or out of order, or events are lost.
- **S3:** availability or liveness.
- **S4:** performance or hygiene.

## 1. Defects, ranked

| # | Sev | Where | Trigger | Impact | Fix |
|---|---|---|---|---|---|
| 1 | S1 | `payouts.py:71` | Every attempt sends `Idempotency-Key: uuid4()`, so a retry is never deduplicated. Retries happen after a timeout (`:74-77`, with a 10 s timeout against hangs of about 25 s *after* commit) and after `502 bad_gateway`, which is also after commit (`:85-86`). | **Double payment.** The retry is a new execution of an already-executed payout. | Use a stable key per payout. Treat any "maybe executed" outcome as *unknown* and resolve it by searching for the reference once the search index is authoritative. Never blindly re-POST (see `settle/payouts.py`). **R1** |
| 2 | S1 | `payouts.py:93-97, 110-111` | The outbox is walked with an `id > last_id` watermark. `bigserial` ids are allocated at INSERT time but become visible at COMMIT time. With 48 concurrent consumer threads, a lower id often commits after a higher one has already been polled. | **Accepted payouts are never sent.** | Claim by state, not position: `WHERE status='pending' … FOR UPDATE SKIP LOCKED`. **R2** |
| 3 | S1 | `payouts.py:91, 111` | The per-replica watermark lives in Redis. Redis was restarted without persistence, so `last_id` falls back to 0. The poll does not filter on payout status (`:93-97`), so every already-paid payout in that shard is sent again, with a fresh uuid key. | **Mass double payment** after any Redis loss. | Keep payout state only in PostgreSQL and claim on `status`. |
| 4 | S1 | `payouts.py:96, 22-23` | Static sharding on `o.id % REPLICAS = REPLICA`, set from env. `docker compose --scale payouts=4` gives every replica the same `REPLICA`, so all four send the same rows. If the env is wrong instead, some shards are never processed, and a dead replica's shard is never processed. | Double payment, or payouts never sent. | Use dynamic claiming with leases (`SKIP LOCKED`). |
| 5 | S1 | `payouts.py:108` | The status is written *after* the POST, with no write-ahead record. A crash between the bank's 201 and this UPDATE leaves `accepted`, so the payout is re-sent with a new key. | Double payment on any executor crash. | Commit `sending` plus `attempt_started_at` before the POST. A `sending` row found with an expired lease goes through reconcile-by-search. |
| 6 | S1 | `payouts.py:83` | Operator precedence: `409 or (422 and error != account_closed)`. `409 request_in_progress` means "a request with this key may still be executing", but it is marked **failed**. | A payout marked failed while the money moves. | Treat `request_in_progress` as ambiguous and reconcile it. |
| 7 | S1 | `payouts.py:102, 94` | `account` and `currency` are read from `merchants` at send time, not captured at the point in the sequence where the payout was accepted. A later `MERCHANT_CONFIG` changes the bank account. | Money is sent to the wrong account. | Snapshot account and currency into the payout row when it is accepted (`ledger.py`). |
| 8 | S1 | `payouts.py:70`, `schema.sql:*` | All money is `double precision` in major units. `int(19.99*100)` is `1998`. The `available()` sums and the comparisons at `ledger_consumer.py:92` are floats too. | Wrong amounts are sent. Balances drift by cents. Funds checks go wrong at the boundary. | Use `bigint` minor units everywhere. |
| 9 | S2 | `ledger_consumer.py:57` (and README) | "Idempotent (`processed_events`)": the table is only *written*, never read. It is also written in autocommit, *before* and outside the effect transaction. | Every redelivery is applied again: CAPTURE credits twice, REFUND and CHARGEBACK debit twice. If the effect fails, the event is still marked processed. | Use a sequence guard on the merchant row, checked under a row lock in the same transaction as the effects. **R4** |
| 10 | S2 | `session_reader.py:61` | Messages are keyed by `event_id`, not `merchant_id`. | A merchant's events are spread over all 12 partitions, so there is **no per-merchant ordering** at all. A REFUND can run before its CAPTURE, a PAYOUT before the CAPTUREs that fund it, and events before `MERCHANT_CONFIG` (FK failure, see #18). | Key by merchant (here: subject = `crc32(merchant) % P`). |
| 11 | S2 | `ledger_consumer.py:18, 127` | A 16-thread pool applies messages from the same partition concurrently and out of order. | Ordering is lost even within a partition. Combined with #12, two payouts of one merchant race. | Apply one partition with one ordered applier, batched. |
| 12 | S2 | `ledger_consumer.py:59, 90-96` | "REPEATABLE READ so concurrent payouts can't overdraw." Two transactions each read `SUM(ledger_entries)` from their own snapshot and INSERT different rows. RR only detects write-write conflicts on the *same* row, so this is write skew. | **Overdraw.** Both payouts are accepted. | Take `SELECT … FOR UPDATE` on the merchant row (or SERIALIZABLE plus retry). **R5** |
| 13 | S2 | `ledger_consumer.py:108` | Offsets are committed asynchronously, per message, from 16 threads, in completion order. Offset N+1 can be committed while N is still running or has failed. | On rebalance or restart (the Kafka rolling restart), N is **skipped forever**. The committed offset can also move *backwards*, causing a redelivery storm that #9 applies twice. | Commit the offset in the same DB transaction as the effects, and resume from it. |
| 14 | S2 | `ledger_consumer.py:106-108, 127` | `apply()` exceptions die inside a discarded `Future`, so nobody sees them. Later messages still commit higher offsets. | Any failing event (FK error, timeout, serialization failure, unknown type) is **silently lost**. | Fail the batch, don't advance the offset, and alert. |
| 15 | S2 | `ledger_consumer.py:77, 88` | `UPDATE platform_fees` touches **one row per currency** for every capture and chargeback of every merchant, at REPEATABLE READ. Concurrent updates raise `could not serialize access`, which hits #14 and the whole capture is lost. The row is also never inserted, so the UPDATE matches nothing and platform fees are always 0. | Captures are lost, mostly for the **busiest** merchants. Fees are wrong. It is also a global hot row. | Keep a per-merchant fee counter and `SUM` it on read (here: `merchants.fees`). |
| 16 | S2 | `session_reader.py:101-103` | All `poss_dup:true` frames are discarded. A *resent* frame always has `poss_dup:true` (`gateway.py:226`), including the one that was lost. All `GAP_FILL`s are discarded too (`:104` is unreachable). | Every frame dropped on the wire (about 0.2 %) is **lost for good**. | Deduplicate on sequence number, never on `poss_dup`. **R6** |
| 17 | S2 | `session_reader.py:107-113` | On a gap it requests a RESEND but **still publishes the out-of-order frame** and advances `expected` and the checkpoint past the hole. | Events are published out of order, and the hole is never filled (see #16). | Use go-back-N: drop frames after the gap until the resend fills it. Re-request if the resend is lost. |
| 18 | S2 | `ledger_consumer.py:62-68` + `schema.sql:11` | `ledger_entries.merchant_id` has an FK to `merchants`, which is created by `MERCHANT_CONFIG`. With #10 and #11, a merchant's AUTH can arrive before its config. | FK violation, then #14: the event is lost. | Order per merchant. Upsert the merchant row on first sight. |
| 19 | S2 | `ledger_consumer.py:70-76` | CAPTURE never checks that the auth is **open** or that `amount ≤ auth amount`. | Double captures and captures of expired auths create money (the acquirer stream contains both). | Implement the rules in §4 exactly (see `settle/ledger.py`, tested against the reference). |
| 20 | S2 | `ledger_consumer.py:79-83` | AUTH_EXPIRY doesn't check the auth is still open. | The hold is released twice, so `held` is wrong or negative. | Same as #19. |
| 21 | S2 | `ledger_consumer.py:84-89` | REFUND doesn't check that the capture exists or for over-refund. CHARGEBACK doesn't check that the capture exists. | Settled balances that are too low. | Same as #19. |
| 22 | S2 | `ledger_consumer.py:100-101` | An unknown event type raises an error, which hits #14. | Breaks forward compatibility. A newer event type (`LOYALTY_ACCRUAL`) is silently dropped. | Ignore unknown types, while still consuming their sequence number. |
| 23 | S2 | `session_reader.py:24-32` | `acks=1`, `enable.idempotence=False`, `max.in.flight=5`, `retries=10`. | A leader change (the rolling restart) **loses acked messages**. Retries **reorder** and **duplicate** them. | Use `acks=all` with idempotence, and grant credit only after the durable ack. |
| 24 | S2 | `session_reader.py:61-62, 112` | `produce()` is fire-and-forget with no delivery callback, and the checkpoint is written right after `produce()`, before any ack. | A crash or broker error after the checkpoint means **events are lost** (resume skips them). | Resume from what is *applied* (the ledger), not from what was handed to a client buffer. |
| 25 | S2 | `session_reader.py:112, 51-57` | Checkpoints and the channel list live in Redis. Redis restart without persistence gives `resume={}`. | A full replay from seq 1, mostly discarded by #16, plus a resend storm. | Make the ledger's `last_seq` the checkpoint. |
| 26 | S2 | `session_reader.py:44` | `reader.read(length)` may return fewer bytes than `length`. The JSON error is caught and "skipped" at `:86-88`, and then the stream is out of frame sync. | Frames are lost and garbage lengths are read until the connection drops. | Use `readexactly`, or a real frame decoder that handles every split (`settle/bosp.py`). |
| 27 | S3 | `leader.py:34-37` + `session_reader.py:83` | Renewal is a blind `PEXPIRE` that never checks ownership. `is_leader` is set once and never cleared. After a Redis restart, or a pause longer than the 15 s TTL, the standby acquires the lock and the old leader carries on (its PEXPIRE even keeps *the new* lock alive). | **Two leaders** that supersede each other at the gateway, and two producers. | Hold the lease in PostgreSQL with an epoch (fencing token) and compare-and-set renewal. Add a local monotonic deadline checked before every publish. **R3** |
| 28 | S3 | `leader.py:45-48` | `release()` deletes the key unconditionally. | It can delete another reader's lock. | Compare-and-delete. |
| 29 | S3 | `session_reader.py:91-92` | `TEST_REQUEST` is answered with a HEARTBEAT that has **no `test_req_id`**, which does not satisfy it (`gateway.py:313`). The reader also never sends heartbeats of its own. | On quiet nights the reader sends nothing for 3 s. The gateway sends `TEST_REQUEST`, it isn't answered, and after 2×hb comes `LOGOUT heartbeat_timeout`. That repeats forever: the **disconnect storms**. | Echo the id. Send our own heartbeat every hb/2. |
| 30 | S3 | `session_reader.py:72-73, 129` | `LOGON_REJECT` (`throttled`, sent when logons are faster than 1/s) raises `RuntimeError`, which the loop does not catch, so **the process dies**. Clean `LOGOUT`s reconnect after only 0.2 s. | Crash loops amplify #29. | Honour `retry_after_ms` and back off. |
| 31 | S3 | `payouts.py:83, 85-86` + `:101-107` | `422 account_closed`, the only permanent failure, falls through to "retry" because of the precedence bug. That retry loop runs inside a **sequential** poll loop. | One payout to a closed account freezes the whole replica: **"stopped doing anything for 40 minutes"**. | Fail it as terminal, and run payouts concurrently. **R7** |
| 32 | S3 | `payouts.py:53-57` | `pg_advisory_lock` / `unlock` are session-level locks used through **PgBouncer transaction pooling**. Lock and unlock can run on different server connections. The unlock fails and the lock stays on a pooled server connection that other clients reuse. | Executors block forever on `pg_advisory_lock` for that merchant hash, another way to freeze. | Never use session state behind transaction pooling. Use row leases instead. |
| 33 | S3 | `ledger_consumer.py:30` | `SET statement_timeout` is a session-level SET through PgBouncer transaction pooling. It leaks onto whichever client next gets that server connection, and is missing from this one. | Random 5 s timeouts in unrelated clients, which then hit #14. | Use `SET LOCAL` in the transaction, or role-level settings. |
| 34 | S3 | `payouts.py:30-36` | Each replica mints its own OAuth token. Issuing one revokes all others, so with 4 replicas each 401s the others. Issuance is throttled to 1 per 5 s, giving a 429 loop. | Throughput collapses under load. | Share one token and let a single minter (behind a lock) replace it with compare-and-delete. |
| 35 | S3 | `payouts.py:39-48` | A fixed-window limiter on each replica's **own clock**, with INCR and EXPIRE non-atomic. At window boundaries it allows about 2× the limit, but the bank uses a sliding window. | 429 storms. | Use an atomic sliding window on Redis server time (`settle/limiter.py`). |
| 36 | S3 | `payouts.py` (whole) | No limit on in-flight payouts per account (the bank allows 2), and nothing shared. | `409 too_many_inflight` is then marked **failed** (#6). | Use a shared per-account semaphore. |
| 37 | S3 | `session_reader.py:112, 54` | A synchronous Redis call per frame inside the asyncio I/O loop. | It blocks heartbeats and throughput (feeds #29). | Keep the I/O loop free of blocking calls. |
| 38 | S4 | `ledger_consumer.py:111-117` | `as_of` compares `event_time::date <= as_of::date`, so it has day granularity, and it ignores `reserve`. | Wrong point-in-time balances. | Store a running balance per entry and do an exact timestamp probe. |
| 39 | S4 | `schema.sql:20` + `ledger_consumer.py:46-51, 113` | The only index is `(event_time)`. `available()` and `as_of` scan all of a merchant's rows (or the whole table). | O(rows) per payout and per query, so at 3 B rows it can't meet the < 200 ms target. | Use an index on `(merchant_id, event_time DESC) INCLUDE (balances)` with a running balance. |
| 40 | S4 | `ledger_consumer.py:55` | A new database connection **per event**. | Connection churn at 30 k/s. | One connection per worker, with batched transactions. |
| 41 | S4 | `schema.sql:11-19` | No uniqueness on `(merchant_id, seq)` or on `event_id`. | Nothing backstops a double apply. | `PRIMARY KEY (merchant_id, seq)`. |

Linter-level items I have not listed separately: the unused `uuid` on the bank side, `log.info` used for rejections, and the missing type checks on frames.

## 2. Root causes of the incident

**14 payouts executed twice, "same reference, minutes apart".** The cause is #1 combined with the bank's two "committed but you don't know it" outcomes.

1. A POST commits, then returns `502`, or hangs about 25 s past the 10 s client timeout.
2. The executor logs "retrying" and backs off `min(30, 2^n)` seconds.
3. It re-POSTs with a **new** uuid key, so the bank executes again.

"Minutes apart" comes from the backoff growing to 30 s and from the **~15 min maintenance window**: a payout that got a 502 just before the window retried 503 after 503 for the whole window, then executed a second time when the bank came back.

Contributing defects:

- #5: an executor crash between the commit and the UPDATE.
- #6: `request_in_progress` marked failed and then re-sent by hand.
- #4 or #3 could produce more, but the shape of the count (14, minutes apart, after ambiguous answers) points at #1.

**3 of the 20 busiest merchants drifted (two too high, one too low).** The ledger has no per-merchant ordering and no real idempotency, and the busiest merchants have the most events in flight at once.

- **Too high:** #23 and #13. The Kafka rolling restart forced a rebalance. Offsets committed out of order (#13) and producer retries without idempotence (#23) both caused **redelivery**. #9 applied redelivered CAPTUREs a second time, crediting `amount − fee` twice.
- **Too low:** #15 and #14. Concurrent captures for a hot merchant hit the platform-fee row under RR. One got `could not serialize access`, the exception vanished (#14), and a later offset commit skipped it (#13): a lost CAPTURE.
- **Also too low:** #12. Two concurrent payouts both passed the funds check, an overdraw the acquirer statement wouldn't show.
- #10 and #11 make all of this likelier exactly where volume is highest. That is why only top-20 merchants show it, and why no single fix explained it.

**9 accepted payouts never sent.** The cause is #2. The consumer inserts payout and outbox rows from 48 concurrent transactions, while the poller advances `last_id` past ids whose transactions hadn't committed yet. Those rows stay `accepted` forever.

- #31 and #32 can *delay* a whole shard (the frozen replica), but delay isn't loss.
- #4 (static sharding) turns a dead replica's shard into loss.

**The ops observations.**

- **Disconnect storms on quiet nights:** #29. With little traffic the reader is silent for over 1.5 × hb, and its answer to `TEST_REQUEST` lacks the id. #30 and #37 amplify it.
- **Two readers "session up" in the same minute:** #27. The Redis node replacement dropped the lock key, the standby acquired it, and the old leader never re-checked `is_leader`.
- **An executor "stopped doing anything" for 40 min:** #31, a closed-account payout retried forever inside the sequential loop, or #32 (a leaked advisory lock behind PgBouncer).

## 3. Reproducers (top 7 by severity)

| | Defect | File | Incumbent result (xfail) | Fix |
|---|---|---|---|---|
| R1 | #1 double payment on ambiguous outcome. Real `harness/bank.py`, every POST commits and then returns 502. | `test_r1_double_payment.py` | bank executed it **3×** in 4 s | executed **1×**: the 502 becomes `unknown`, then search finds it, then `paid` |
| R2 | #2 outbox watermark skips a late-committing row (real PostgreSQL, incumbent `schema.sql`, the poll query verbatim) | `test_r2_outbox_skip.py` | only `P-late` is sent | both are claimed |
| R3 | #27 split brain after a Redis restart (real Redis, incumbent `leader.py` unmodified) | `test_r3_split_brain.py` | 2 leaders | 1. A paused leader's CAS renewal fails and its local deadline has passed. |
| R4 | #9 redelivered CAPTURE applied twice (incumbent `ledger_consumer.apply` against real PostgreSQL) | `test_r4_redelivery_double_apply.py` | settled **19 360** vs 9 680 | 9 680 |
| R5 | #12 write skew at REPEATABLE READ (the race forced with a barrier) | `test_r5_payout_write_skew.py` | **2** of 2 payouts accepted with funds for 1 | 1. The second payout waits on the row lock, re-reads and is rejected. |
| R6 | #16 + #17 resent frame discarded, out-of-order publish | `test_r6_resend_loss.py` | `[1, 2, 4, 5, 5]` | `[1, 2, 3, 4, 5]` |
| R7 | #31 closed account retried forever, freezing the replica (real bank) | `test_r7_closed_account_stall.py` | no result after 3 s (still looping) | `failed` on the first answer |

How the reproducers load the incumbent:

- The incumbent modules are loaded **unmodified**, with `confluent_kafka` stubbed.
- In R1 and R7 only the backoff sleeps are shortened, and the Redis rate limiter is bypassed because it isn't what's under test.
- R2 and R6 extract incumbent logic verbatim into a small unit, as the brief allows.
