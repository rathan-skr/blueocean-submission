# Settlement Stream: Blue Ocean backend assignment v2

**What this is.** A payments settlement pipeline:

- It reads the BOSP/1 gateway and fans events out through **NATS JetStream**, partitioned by merchant.
- It applies them **exactly once** to a **PostgreSQL** ledger.
- It executes accepted payouts at the bank **exactly once**, including when the outcome is ambiguous.

The documents: `DESIGN.md` (Part B), `REVIEW.md` (Part C), `AI-USAGE.md`.

## Run it

You need Docker with Compose v2.20 or newer (for `include:`). The host only needs Python 3.11+ to run `verify.py`.

```bash
docker compose up --build -d                     # everything, harness included; no manual steps
python harness/verify.py watch                   # live backlog
python harness/verify.py check                   # after the gateway reports finished -> RESULT: PASS
```

**Harsher, review-like settings.** These are §5.4 items 8 and 9: 5× drops, disconnects every 20–40 s, a 20 s idempotency TTL, 30 s maintenance every 90 s, and 10 % after-commit failures.

```bash
GATEWAY_EVENTS_PER_CHANNEL=10000 GATEWAY_RATE=3000 \
  docker compose -f docker-compose.yml -f docker-compose.review.yml up --build -d
python scripts/faults.py all          # the §5.4 fault schedule against the running stack
python harness/verify.py check
```

**Clean run.** The gateway's release clock starts when it starts, so reset everything together: `docker compose down -v && docker compose up --build -d`.

**Tests.** These use real PostgreSQL and Redis from the stack, and the real `harness/bank.py`.

```bash
docker compose --profile test run --rm tests                 # all: 45 passed, 7 xfailed
docker compose --profile test run --rm tests -k incumbent    # Part C reproducers only
```

## Services and how to scale them

| Service | Replicas | Scale with | Notes |
|---|---|---|---|
| `reader` | 2 | `--scale reader=N` | One leader holds the gateway session. The others are hot standbys polling a PostgreSQL lease. More replicas only add standbys. |
| `consumer` | 4 | `--scale consumer=N` | Partitions (16 subjects) rebalance automatically in about 2–10 s. More than 16 consumers adds nothing. |
| `payouts` | 2 | `--scale payouts=N` | Payouts are claimed with `SKIP LOCKED` leases. The bank limits (rate, tokens, 2 in flight per account) are shared through Redis. |
| `api` | 1 | publish a different port per replica | Stateless. `:8080`. |
| `postgres`, `nats`, `redis` | 1 | n/a | Redis is deliberately not durable. |
| `gateway`, `bank` | 1 | n/a | Harness, included unmodified from `harness/docker-compose.harness.yml`. |

**Operator endpoints** (on the API, `:8080`):

| Endpoint | What it answers |
|---|---|
| `GET /v1/ops/status` | Who holds the session and since when, with lease epoch and gateway `session_epoch`. Which channels are behind and by how much. Reader resend and gap counters. Broker lag per partition, with owner and epoch. Live consumers. Payouts by state, including the ambiguous ones (`unknown`/`sending`). Bank response classes. |
| `GET /metrics` | The same, in Prometheus format. |
| `GET /healthz` | Liveness. |
| `GET /readyz` | Readiness. Checks PostgreSQL. |

Logs are JSON lines carrying `event_id`, `payout_id`, `session_epoch`, `lease_epoch` and `partition` where relevant (`docker compose logs -f reader`).

## How it works

```
gateway ──BOSP──▶ reader ×2 (1 leader: PG lease + epoch) ──publish, PubAck──▶ JetStream "LEDGER"
   ▲   CREDIT returned only after PubAck                                        subjects ledger.p.0..15
   └── resume = merchants.last_seq (what is APPLIED)                             (crc32(merchant) % 16)
                                                                                       │ pull from offset+1
                       consumer ×4: one txn per batch = fence(partition owner, epoch) │
                        + per-merchant guard (seq = last_seq+1) + effects + offset  ◀──┘
                                                │
                                   PostgreSQL ──┤ payouts row created in the same txn
                                                ▼
          payouts ×2: claim (SKIP LOCKED lease) → write-ahead 'sending' → POST (stable key)
                      ambiguous → 'unknown' → wait until any commit is visible → SEARCH → paid | resend
```

The DESIGN.md sections that cover each mechanism:

- **Delivery semantics at every hop:** DESIGN.md §1.
- **Fencing of a paused leader:** §2.
- **Credit versus durability:** §3. A channel's credit is returned to the gateway only when JetStream has durably acknowledged the frame, or the frame is known to be redundant. So the gateway can never run more than `CREDIT_WINDOW` frames per channel ahead of what the broker holds. Everything the broker holds is replayable, and the resume point is the ledger itself.

**Broker choice: NATS JetStream.**

- It runs as one small container that starts in about a second, which helps the "< 10 min on a clean machine" requirement.
- It has durable file-backed streams (Raft R3 in production).
- It has per-subject ordering, and synchronous PubAcks that we tie credit to.
- It has pull consumers that start at any stream sequence. That lets the **offset live in PostgreSQL, in the same transaction as the effects**, so we need none of the broker's consumer-group or ack machinery.
- Kafka would also work but is heavier to run here. Redis Streams was out, because Redis must not be a durability dependency.

## Built vs deferred

**Built.** Every Part A requirement (A1–A7):

- **Leader election and failover:** about 8–10 s after `kill -9`. A paused leader is fenced.
- **BOSP:** framing, heartbeats and `TEST_REQUEST`, go-back-N resend with lost-resend detection, a tail-loss idle probe, `GAP_FILL`, duplicate handling, resume, and credit tied to PubAck.
- **Consumers:** partition leases with an epoch fence. The offset is committed in the apply transaction.
- **The exact ledger rules:** tested against the reference on 120 k frames.
- **Platform fees:** sharded per merchant, so there is no hot row.
- **Point-in-time balances:** `as_of` is a single index-only probe, p99 about 4 ms over HTTP locally.
- **Payout executor:**
  - write-ahead intent before each POST;
  - an outcome matrix covering every row of BANK-API.md;
  - a reconcile-by-search ambiguity protocol;
  - shared token, rate limit and per-account in-flight limit;
  - crash-safe at every step.
- **Read API and ops views, metrics, health and readiness checks.**
- **Fault script, and a test suite against real PostgreSQL and Redis.**

**Deferred, and why.**

- **Poison-event quarantine.** Today a frame that makes a batch fail stalls its partition, retries, and shows up as partition lag. It is never skipped, because skipping it would silently corrupt that merchant's balance. Bisecting the batch to isolate the bad frame and parking only that merchant is designed in DESIGN.md §8 but not built.
- **Adaptive credit for 60 k channels.** The fixed per-channel window of 1000 fits 200 channels. DESIGN.md §3 describes the budgeted scheme production needs.
- **Multiple `client_id` sessions, hot-merchant partition overrides, and monthly partitioning plus snapshots for 7-year retention.** These are designed (DESIGN.md §4–5) but not needed at assignment scale.
- **A daily payout reconciliation job** (every paid payout has exactly one bank execution). Detection exists inline: any search that finds more than one execution logs CRITICAL. The batch job is designed in DESIGN.md §7.

## Known issues

- **Throughput was measured on a 2-core sandbox,** with gateway, bank, verifier and the whole stack on the same cores. There the pipeline kept pace with the gateway's actual release rate, which the gateway itself held to about 1.5–2.5 k frames/s on that box. I could not measure the 8-core review machine. Section 5 of DESIGN.md has the per-component numbers.
- **The reader is a single Python process.** At 12 k frames/s bursts, the 3000/s review rate × 4, it relies on credit backpressure: the backlog stays at the gateway and is caught up after the burst.
- **`docker kill -s STOP` versus `docker pause`.** Both freeze the leader. With `restart: unless-stopped`, Docker restarts killed containers, which is fine: they come back as standbys.
- **Bank rate limit assumption.** BANK-API.md says 20 POST/s and 10 searches/s, while ASSIGNMENT §2 says 50 req/s. I coded to the bank's own contract, at 18 and 8 for headroom. Both are configurable (`BANK_RATE_LIMIT`, `BANK_SEARCH_RATE_LIMIT`).

## Time spent (honest)

| Area | Time |
|---|---|
| Reading the brief, harness source and incumbent; architecture and delivery semantics | 0:50 |
| Environment (Docker), test rig | 0:20 |
| Reader + consumers + ledger + API, passing `verify` with chaos | 0:50 |
| Payout executor + limiter + outcome matrix | 0:30 |
| Fault-injection runs (500 k and 2 M frames, all §5.4 faults) and fixes | 0:25 |
| REVIEW.md + 7 reproducers | 0:45 |
| DESIGN.md | 0:40 |
| Packaging, README, AI-USAGE | 0:20 |
