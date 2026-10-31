# AI-USAGE.md

## Tools and what they were used for

| Tool | Used for |
|---|---|
| **Claude (Anthropic), as an agentic coding assistant** | Reading the brief and the harness and incumbent source together with me. Drafting the code, tests, REVIEW.md and DESIGN.md to the architecture I set. Running the stack and the fault schedule on a Linux sandbox (native PostgreSQL 16, NATS 2.10 and Redis 7) and reading the results back. |
| Docker Desktop (my laptop) | Running the compose stack as the reviewers will. |

**How I worked with it.** I fixed the architecture and the delivery semantics *before* any code. That covered:

- resuming from the applied `last_seq`;
- returning credit only on PubAck;
- keeping the offset in the apply transaction;
- the per-merchant sequence guard as the real fence;
- write-ahead plus reconcile-by-search for payouts.

After that, every component was judged against the harness **source**, not just the prose specs: `gateway.py`, `bank.py`, and `ledger_events.apply_event`. That mattered for these behaviours:

- `poss_dup` being set from the gateway's `seen` map;
- resent NOTICE runs coming back as a GAP_FILL;
- dropped frames not consuming credit;
- `idem[key]["at"]` keeping the *first* request time;
- the bank's commit happening *before* the 25 s hang.

Nothing was accepted on "it passes": every claim was checked against `verify.py` under fault injection, or against a test that fails for the right reason.

## Three places the AI was wrong, and how I noticed

**1. A Part C reproducer that "proved" the bug for the wrong reason.**

- **What happened.** The first version of R1 (the double payment) showed `XFAIL` for the incumbent on the first run. That looked like a reproduction.
- **How I noticed.** I re-ran it with `--runxfail` to see the actual failure: it was `ModuleNotFoundError: No module named 'requests'`. The incumbent never ran.
- **The fix.** Every incumbent case is now `xfail(strict=True, raises=AssertionError)`, so only a failed *property* counts as reproduced. I re-checked each reproducer's real assertion: R1 now shows the bank executing the payout **3×**, and R4 shows settled at **19 360 vs 9 680**.

**2. The shared OAuth token could never be invalidated.**

- **What happened.** The first `TokenCache` stored `[token, expiry]` as JSON in Redis. Its compare-and-delete then compared that JSON string against the raw token.
- **The effect.** On a `401 invalid_token`, a stale token would have stayed in Redis until its own TTL. Every executor would keep sending it and keep getting 401: a throughput collapse that would look like a bank problem.
- **How I noticed.** Reviewing the code path for a revoked token before running it.
- **The fix.** A Lua script that deletes the key only if it still contains *that* token.

**3. Cancelling a partition task mid-transaction.**

- **What happened.** The first consumer called `task.cancel()` when a partition lease was lost. The apply runs in a worker thread (`asyncio.to_thread`), and cancelling the awaiting task does **not** stop that thread. The `finally:` block would then close the psycopg connection *while the thread was mid-transaction on it*. That is undefined behaviour for a non-thread-safe connection.
- **How I noticed.** Reviewing the rebalance path for the "consumer killed mid-batch / rebalance" fault.
- **The fix.** Lease loss now only *flags* the partition. The loop finishes or aborts its current batch (which is fenced on `owner_epoch` anyway) and releases cleanly.

**A design suggestion I rejected.** The AI's first broker setup used JetStream `Nats-Msg-Id` deduplication, which is "exactly-once publishing" on paper. Tracing it against the consumer's gap rule showed it creates a **permanent hole**:

1. A consumer drops frame *n+1* because *n* hasn't arrived yet.
2. The reader reconnects and re-publishes *n* and *n+1*.
3. The broker silently discards the re-published *n+1* as a duplicate inside its 2-minute window.

I removed it and made the ledger's sequence guard the only dedup (DESIGN.md §1).
