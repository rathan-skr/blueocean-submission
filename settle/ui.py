"""The settlement console: a browser UI at /ui for understanding and testing the running system.

Read-only. Everything shown comes from the same sources as the operator API (PostgreSQL, plus
reader counters from Redis). Nothing here is used by the pipeline itself.
"""
from __future__ import annotations

import json
import pathlib

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

router = APIRouter()
_HTML = pathlib.Path(__file__).parent / "ui" / "index.html"


async def _q(sql, args=(), one=False):
    from .api import q                         # lazy: api imports this module at the end
    return await q(sql, args, one)


async def _redis_json(key):
    from .api import _redis_json as rj
    return await rj(key)


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def ui_page():
    return HTMLResponse(_HTML.read_text(encoding="utf-8"))


@router.get("/v1/ui/summary")
async def summary():
    """Live numbers for the pipeline strip: one cheap query per stage."""
    lease = await _q("SELECT holder, epoch, session_epoch, expires_at > now(), extract(epoch from now() - since) "
                     "FROM reader_lease WHERE name = 'bosp'", one=True)
    reader = await _redis_json("reader:status") or {}
    parts = await _q("SELECT count(*) FILTER (WHERE owner IS NOT NULL AND lease_until > now()), count(*), "
                     "coalesce(sum(lag), 0), coalesce(sum(applied), 0), coalesce(sum(dup_skipped), 0), "
                     "coalesce(sum(gap_dropped), 0) FROM partitions", one=True)
    members = await _q("SELECT count(*) FROM consumer_members WHERE heartbeat > now() - interval '6 seconds'", one=True)
    ledger = await _q("SELECT count(*), coalesce(sum(last_seq), 0), coalesce(sum(fees), 0) FROM merchants", one=True)
    payouts = dict(await _q("SELECT status, count(*) FROM payouts GROUP BY status"))
    bank = dict(await _q("SELECT outcome, count(*) FROM payout_attempts WHERE kind = 'post' GROUP BY outcome"))
    searches = dict(await _q("SELECT outcome, count(*) FROM payout_attempts WHERE kind = 'search' GROUP BY outcome"))
    frames = reader.get("frames", {})
    return {
        "gateway": {"session_epoch": lease[2], "frames_received": sum(frames.values()) if frames else 0,
                    "frames_by_type": frames, "resends": reader.get("resends", 0),
                    "idle_probes": reader.get("idle_probes", 0), "sessions": reader.get("sessions", 0)},
        "reader": {"holder": lease[0], "lease_epoch": lease[1], "lease_live": lease[3],
                   "leader_for_s": round(lease[4] or 0), "published": reader.get("published", 0),
                   "dups": reader.get("dups", 0), "gap_frames": reader.get("gap_frames", 0),
                   "puback_timeouts": reader.get("puback_timeouts", 0)},
        "broker": {"partitions": parts[1], "lag": int(parts[2])},
        "consumers": {"live": members[0], "owned_partitions": parts[0], "applied": int(parts[3]),
                      "duplicates_skipped": int(parts[4]), "gaps_dropped": int(parts[5])},
        "ledger": {"merchants": ledger[0], "frames_applied": int(ledger[1]), "platform_fees_minor": int(ledger[2])},
        "payouts": payouts,
        "bank": {"posts": bank, "searches": searches},
    }


@router.get("/v1/ui/merchants")
async def merchants(q: str = "", limit: int = Query(50, le=500), order: str = "id"):
    by = {"id": "merchant_id", "busy": "last_seq DESC", "recent": "updated_at DESC",
          "settled": "settled DESC"}.get(order, "merchant_id")
    rows = await _q(f"SELECT merchant_id, currency, settled, held, settled - reserve, reserve, last_seq, bank_account, "
                    f"updated_at FROM merchants WHERE merchant_id ILIKE %s ORDER BY {by} LIMIT %s",
                    (f"%{q}%", limit))
    return [{"merchant_id": r[0], "currency": (r[1] or "").strip(), "settled": r[2], "held": r[3], "available": r[4],
             "reserve": r[5], "last_seq": r[6], "bank_account": r[7], "updated_at": r[8]} for r in rows]


@router.get("/v1/ui/merchants/{merchant_id}/entries")
async def entries(merchant_id: str, limit: int = Query(50, le=500), before_seq: int | None = None):
    """The merchant's journal, newest first: every DATA event with its result and the balance after it."""
    rows = await _q("SELECT seq, event_id, event_type, event_time, result, amount, settled_after, held_after, "
                    "reserve_after FROM ledger_entries WHERE merchant_id = %s AND seq < coalesce(%s, 9223372036854775807) "
                    "ORDER BY seq DESC LIMIT %s", (merchant_id, before_seq, limit))
    return [{"seq": r[0], "event_id": r[1], "type": r[2], "event_time": r[3], "result": r[4], "amount": r[5],
             "settled_after": r[6], "held_after": r[7], "available_after": r[6] - r[8]} for r in rows]


@router.get("/v1/ui/payouts")
async def payouts(status: str = "", merchant: str = "", limit: int = Query(100, le=1000)):
    rows = await _q("SELECT payout_id, merchant_id, account, currency, amount, status, reason, bank_payout_id, attempts, "
                    "key_gen, updated_at FROM payouts WHERE (%s = '' OR status = %s) AND merchant_id ILIKE %s "
                    "ORDER BY updated_at DESC LIMIT %s", (status, status, f"%{merchant}%", limit))
    return [{"payout_id": r[0], "merchant_id": r[1], "account": r[2], "currency": (r[3] or "").strip(), "amount": r[4],
             "status": r[5], "reason": r[6], "bank_payout_id": r[7], "attempts": r[8], "key_gen": r[9],
             "updated_at": r[10]} for r in rows]


@router.get("/v1/ui/payouts/{payout_id}")
async def payout_detail(payout_id: str):
    """One payout plus every exchange with the bank, oldest first."""
    p = await _q("SELECT payout_id, merchant_id, account, currency, amount, status, reason, bank_payout_id, attempts, "
                 "key_gen, attempt_started_at, next_attempt_at, lease_owner, created_at, updated_at "
                 "FROM payouts WHERE payout_id = %s", (payout_id,), one=True)
    if p is None:
        raise HTTPException(404, "no such payout (yet)")
    attempts = await _q("SELECT at, kind, idem_key, outcome, http_status, executor FROM payout_attempts "
                        "WHERE payout_id = %s ORDER BY id", (payout_id,))
    keys = ["payout_id", "merchant_id", "account", "currency", "amount", "status", "reason", "bank_payout_id",
            "attempts", "key_gen", "attempt_started_at", "next_attempt_at", "lease_owner", "created_at", "updated_at"]
    out = dict(zip(keys, p))
    out["currency"] = (out["currency"] or "").strip()
    out["history"] = [{"at": a[0], "kind": a[1], "idem_key": a[2], "outcome": a[3], "http_status": a[4],
                       "executor": a[5]} for a in attempts]
    return out


@router.get("/v1/ui/checks")
async def checks():
    """Self-checks a tester can run against the live database at any time. Each one is an invariant
    the design promises; a failure points at the component to look at."""
    out = []

    def add(name, ok, detail, where):
        out.append({"name": name, "ok": bool(ok), "detail": detail, "where": where})

    lease = await _q("SELECT holder, expires_at > now() FROM reader_lease WHERE name = 'bosp'", one=True)
    add("A reader holds the gateway session", lease[1], f"holder: {lease[0] or 'nobody'}", "reader.py, Lease")

    p = await _q("SELECT count(*) FILTER (WHERE owner IS NULL OR lease_until < now()), count(*) FROM partitions", one=True)
    add("Every broker partition has a live consumer", p[0] == 0, f"{p[0]} of {p[1]} partitions unowned",
        "consumer.py, coordinate()")

    bad = await _q("SELECT count(*) FROM merchants m JOIN LATERAL (SELECT settled_after, held_after FROM ledger_entries e "
                   "WHERE e.merchant_id = m.merchant_id ORDER BY seq DESC LIMIT 1) l ON true "
                   "WHERE l.settled_after <> m.settled OR l.held_after <> m.held", one=True)
    add("Each merchant's balance equals its last journal entry", bad[0] == 0, f"{bad[0]} merchants disagree",
        "ledger.py, apply_batch() step 4")

    gap = await _q("SELECT count(*) FROM (SELECT merchant_id, max(seq) AS mx, count(*) AS n FROM ledger_entries "
                   "GROUP BY merchant_id) x JOIN merchants m USING (merchant_id) WHERE x.mx > m.last_seq", one=True)
    add("No journal entry is ahead of its channel's last_seq", gap[0] == 0, f"{gap[0]} merchants ahead",
        "ledger.py, the seq guard")

    neg = await _q("SELECT count(*) FROM merchants WHERE held < 0", one=True)
    add("No negative holds", neg[0] == 0, f"{neg[0]} merchants with held < 0", "ledger.py, AUTH_EXPIRY / CAPTURE")

    dbl = await _q("SELECT count(*) FROM payouts WHERE reason = 'double_payment_detected'", one=True)
    add("No double payment detected by reconciliation", dbl[0] == 0, f"{dbl[0]} payouts flagged",
        "payouts.py, reconcile()")

    stuck = await _q("SELECT count(*) FROM payouts WHERE status IN ('sending', 'unknown') "
                     "AND updated_at < now() - interval '10 minutes'", one=True)
    add("No payout stuck in an ambiguous state for 10+ minutes", stuck[0] == 0, f"{stuck[0]} stuck",
        "payouts.py, reconcile() (bank maintenance can delay this)")

    paid_no_id = await _q("SELECT count(*) FROM payouts WHERE status = 'paid' AND bank_payout_id IS NULL", one=True)
    add("Every paid payout has a bank payout id", paid_no_id[0] == 0, f"{paid_no_id[0]} without id",
        "payouts.py, send() / reconcile()")
    return out
