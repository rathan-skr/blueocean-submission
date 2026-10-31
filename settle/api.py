"""Read API (the contract verify.py uses) plus the operator views."""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

import psycopg
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from psycopg_pool import AsyncConnectionPool

from . import config, db
from .log import setup as setup_logging

log = logging.getLogger("api")
pool: AsyncConnectionPool
rds: aioredis.Redis


@asynccontextmanager
async def lifespan(app):
    global pool, rds
    setup_logging("api")
    db.bootstrap()
    pool = AsyncConnectionPool(config.DATABASE_URL, min_size=4, max_size=20, open=False,
                               kwargs={"autocommit": True, "application_name": "api",
                                       "options": "-c timezone=UTC"}, timeout=5)   # timestamps out in UTC
    await pool.open(wait=False)
    rds = aioredis.from_url(config.REDIS_URL, socket_timeout=1, socket_connect_timeout=1)
    yield
    await pool.close()


app = FastAPI(title="settlement read API", lifespan=lifespan)


async def q(sql: str, args=(), one=False):
    async with pool.connection() as conn:
        cur = await conn.execute(sql, args)
        return await (cur.fetchone() if one else cur.fetchall())


# -- contract -----------------------------------------------------------------------------------
@app.get("/v1/merchants/{merchant_id}/balance")
async def balance(merchant_id: str, as_of: str | None = Query(default=None)):
    """Current balance, or the balance after the last event with event_time <= as_of (one index probe)."""
    if as_of is None:
        r = await q("SELECT currency, settled, held, reserve, last_seq FROM merchants WHERE merchant_id = %s",
                    (merchant_id,), one=True)
        if r is None:
            raise HTTPException(404, "unknown merchant")
        return {"merchant_id": merchant_id, "currency": (r[0] or None), "settled_minor": r[1], "held_minor": r[2],
                "available_minor": r[1] - r[3], "last_seq": r[4]}
    try:
        # Point in time: the journal row of the LAST event at or before as_of carries the balance after it.
        # Served by index ledger_entries_asof (merchant_id, event_time DESC, seq DESC) INCLUDE (balances):
        # one index-only probe, however many rows the table has. No row = before the first event = zeros.
        r = await q("SELECT settled_after, held_after, reserve_after FROM ledger_entries "
                    "WHERE merchant_id = %s AND event_time <= %s::timestamptz "
                    "ORDER BY event_time DESC, seq DESC LIMIT 1", (merchant_id, as_of), one=True)
    except psycopg.errors.DataError:
        raise HTTPException(400, "as_of must be an ISO-8601 timestamp")
    s, h, res = r if r else (0, 0, 0)
    return {"merchant_id": merchant_id, "settled_minor": s, "held_minor": h, "available_minor": s - res}


@app.get("/v1/platform/balances")
async def platform_balances():
    # Platform fees are kept per merchant (no single hot row every capture would contend on); sum on read.
    rows = await q("SELECT currency, sum(fees)::bigint FROM merchants WHERE currency IS NOT NULL GROUP BY currency")
    return {cur.strip(): int(total) for cur, total in rows if total}


# Internal states 'sending' and 'unknown' are still "not finished" to the outside world.
_PUBLIC_STATUS = {"pending": "pending", "sending": "pending", "unknown": "pending",
                  "paid": "paid", "failed": "failed", "rejected": "rejected"}


@app.get("/v1/payouts/{payout_id}")
async def payout(payout_id: str):
    r = await q("SELECT status, bank_payout_id, reason FROM payouts WHERE payout_id = %s", (payout_id,), one=True)
    if r is None:
        raise HTTPException(404, "payout not applied yet")
    return {"payout_id": payout_id, "status": _PUBLIC_STATUS[r[0]], "bank_payout_id": r[1], "reason": r[2]}


# -- health -------------------------------------------------------------------------------------
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    try:
        await q("SELECT 1", one=True)
    except Exception as e:
        return JSONResponse({"status": "not_ready", "postgres": repr(e)[:200]}, status_code=503)
    return {"status": "ready", "postgres": "ok"}


# -- operator views -----------------------------------------------------------------------------
async def _redis_json(key):
    try:
        v = await rds.get(key)
        return json.loads(v) if v else None
    except Exception:
        return None


@app.get("/v1/ops/status")
async def ops_status(behind_limit: int = 20):
    """Operator view: session holder/epochs, channels behind, broker lag per partition, consumers,
    payouts by state (incl. ambiguous), bank response classes."""
    lease = await q("SELECT holder, epoch, since, expires_at, session_epoch, expires_at > now() FROM reader_lease "
                    "WHERE name = 'bosp'", one=True)
    reader = await _redis_json("reader:status")
    chans = await _redis_json("reader:channels") or {}
    applied = dict(await q("SELECT merchant_id, last_seq FROM merchants"))
    behind = []
    for ch, s in chans.items():
        lag = max(0, s["expected"] - 1 - applied.get(ch, 0))     # received in order, not yet applied
        if lag or s.get("expected", 0) <= s.get("highest_seen", 0):
            behind.append({"channel": ch, "applied": applied.get(ch, 0), "received": s["expected"] - 1,
                           "highest_seen": s["highest_seen"], "apply_lag": lag,
                           "receive_gap": max(0, s["highest_seen"] - (s["expected"] - 1))})
    behind.sort(key=lambda x: -(x["apply_lag"] + x["receive_gap"]))
    parts = await q("SELECT partition, owner, owner_epoch, lease_until > now(), stream_seq, lag, applied, dup_skipped, "
                    "gap_dropped FROM partitions ORDER BY partition")
    members = await q("SELECT member_id, heartbeat, heartbeat > now() - make_interval(secs => %s) FROM consumer_members "
                      "ORDER BY member_id", (config.MEMBER_TTL_S,))
    payouts = dict(await q("SELECT status, count(*) FROM payouts GROUP BY status"))
    outcomes = await q("SELECT kind, outcome, count(*) FROM payout_attempts GROUP BY kind, outcome ORDER BY 1, 3 DESC")
    stuck = await q("SELECT payout_id, status, attempts, updated_at FROM payouts WHERE status IN ('unknown', 'sending') "
                    "ORDER BY updated_at LIMIT 20")
    return {
        "session": {"holder": lease[0], "lease_epoch": lease[1], "since": lease[2], "lease_expires": lease[3],
                    "gateway_session_epoch": lease[4], "lease_live": lease[5]},
        "reader": reader,
        "channels_behind": behind[:behind_limit],
        "channels_behind_count": len(behind),
        "broker_partitions": [{"partition": p, "owner": o, "owner_epoch": e, "lease_live": live, "offset": s,
                               "lag": lag, "applied": a, "dup_skipped": d, "gap_dropped": g}
                              for p, o, e, live, s, lag, a, d, g in parts],
        "consumers": [{"member": m, "heartbeat": h, "live": live} for m, h, live in members],
        "payouts_by_state": payouts,
        "payouts_ambiguous": stuck,
        "bank_outcomes": [{"kind": k, "outcome": o, "count": n} for k, o, n in outcomes],
    }


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Prometheus text format, computed from the system of record (and Redis for reader counters)."""
    out = []

    def g(name, value, **labels):
        lab = ",".join(f'{k}="{v}"' for k, v in labels.items())
        out.append(f"{name}{{{lab}}} {value}" if lab else f"{name} {value}")

    lease = await q("SELECT epoch, coalesce(session_epoch, 0), extract(epoch from since) FROM reader_lease "
                    "WHERE name = 'bosp'", one=True)
    g("settle_reader_lease_epoch", lease[0])
    g("settle_gateway_session_epoch", lease[1])
    g("settle_reader_leader_since_seconds", lease[2] or 0)
    reader = await _redis_json("reader:status") or {}
    for k in ("published", "dups", "gap_frames", "resends", "idle_probes", "test_requests", "silence_timeouts",
              "puback_timeouts", "sessions"):
        g(f"settle_reader_{k}_total", reader.get(k, 0))
    for p, lag, applied, d, gp in await q("SELECT partition, lag, applied, dup_skipped, gap_dropped FROM partitions"):
        g("settle_broker_partition_lag", lag, partition=p)
        g("settle_consumer_applied_total", applied, partition=p)
        g("settle_consumer_duplicates_total", d, partition=p)
        g("settle_consumer_gap_dropped_total", gp, partition=p)
    for st, n in await q("SELECT status, count(*) FROM payouts GROUP BY status"):
        g("settle_payouts", n, status=st)
    for kind, o, n in await q("SELECT kind, outcome, count(*) FROM payout_attempts GROUP BY 1, 2"):
        g("settle_bank_responses_total", n, kind=kind, outcome=o)
    return "\n".join(out) + "\n"


def main():
    import uvicorn
    uvicorn.run("settle.api:app", host="0.0.0.0", port=config.API_PORT, log_level="warning", access_log=False)



# The settlement console (/ui) and its read-only JSON endpoints live in ui.py.
from .ui import router as _ui_router  # noqa: E402  (imported last: ui.py uses q() defined above)

app.include_router(_ui_router)

if __name__ == "__main__":
    main()
