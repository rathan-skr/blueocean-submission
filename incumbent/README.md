# Incumbent settlement pipeline (v0.9): for Part C

This is the code the previous team shipped for the same problem you are solving. **Do not build on it.** Your job in Part C is to review it.

## Architecture as they described it

```
gateway ──BOSP──▶ session_reader.py (2 replicas, Redis leader lock)
                      │ produce(ledger-events)
                      ▼
                   Kafka (12 partitions, RF=3)
                      │ consumer group "ledger"
                      ▼
               ledger_consumer.py (3 replicas × 16 threads) ──▶ PostgreSQL (via PgBouncer, transaction pooling)
                                                                    │ payout_outbox
                                                                    ▼
                                                   payouts.py (4 replicas) ──▶ settlement bank
```

Their design notes, verbatim from the wiki:

> - Leader election through Redis so only one reader talks to the gateway.
> - Checkpoints in Redis so a new leader resumes where the old one stopped.
> - Ledger consumers are idempotent (`processed_events`) so Kafka redelivery is safe.
> - Payout funds check runs at REPEATABLE READ so concurrent payouts can't overdraw.
> - Payouts use the transactional outbox pattern, so a payout row and its outbox row commit atomically.
> - All bank calls send an `Idempotency-Key`.
> - The rate limiter is global (Redis), so 4 replicas together stay under the bank limit.

## What happened

The pipeline ran in production for two weeks. Then it was rolled back.

**Findings at rollback:**
- **14 payouts** were executed **twice** at the bank. The bank's statement shows two executions with the same `reference`, minutes apart.
- **3 merchants** (all among the 20 busiest) had balances that did not match the acquirer's end-of-day statement. Two were too high; one was too low.
- **9 payouts** were in status `accepted` in our database but had **never been sent** to the bank. The merchants complained.
- Operations also reported:
  - "Gateway disconnect storms" of several hundred reconnects an hour on quiet nights;
  - two occasions when both reader replicas logged `session up` within the same minute;
  - one payout executor replica that "stopped doing anything" for 40 minutes until it was restarted.

**Events during the two weeks:**
- one Redis restart (a node replacement, so persistence was lost);
- one Kafka broker rolling restart;
- one bank maintenance window of ~15 minutes;
- PgBouncer was in front of PostgreSQL the whole time.

The post-mortem was inconclusive. Each of the "obvious" fixes proposed at the time explained at most one symptom.

## Files

| File | What it is |
|---|---|
| `schema.sql` | Database schema |
| `leader.py` | Redis-based leader election for the session reader |
| `session_reader.py` | BOSP client → Kafka producer |
| `ledger_consumer.py` | Kafka consumer → PostgreSQL ledger, and the `as_of` balance query |
| `payouts.py` | Outbox poller → bank client, plus the rate limiter |

The code imports `confluent_kafka`, `redis`, `psycopg` (v3) and `requests`. It is not wired into a compose file. To build reproducers, extract the logic you need into testable units, or run pieces of it against the services in your own compose.
