"""Payout executor. Run >= 2 processes.

State machine (payouts.status):

    pending --claim+write-ahead--> sending --POST--> paid                  (201 / replayed 200)
                                          \\------> failed                (422 account_closed: never retried)
                                          \\------> pending (+delay)      (definitive "not executed":
                                          |                               401, 429, 409 inflight, 503, connect error)
                                          \\------> unknown               (502, timeout, 409 in_progress, ...)
    sending | unknown --wait until any commit must be visible--> SEARCH by reference
                                          found 1   -> paid
                                          found >1  -> paid + DOUBLE PAYMENT alert (detection, not prevention)
                                          found 0   -> pending with a NEW idempotency key (key_gen + 1)

Why not just retry with the same Idempotency-Key? The bank forgets keys after 120 s (20 s in the
review run), maintenance windows are longer than that, and a request can hang for 25 s *after*
committing. A same-key retry after the key expired is a brand new payout. So an ambiguous attempt
is only ever resolved by the bank's own record (search), and only once that record is
authoritative: attempt start + request timeout + commit grace + search-index lag.

Crash safety: `sending` and `attempt_started_at` are committed BEFORE the POST leaves this process.
Whoever finds a `sending` row with an expired lease (its executor died) treats it exactly like
`unknown`. Every row update is fenced on lease_owner.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import time

import psycopg
import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool

from . import config, db
from .bankapi import Bank, Kind, Outcome
from .limiter import AccountSlots, SlidingWindow, TokenCache
from .log import setup as setup_logging

log = logging.getLogger("payouts")
ME = config.INSTANCE_ID
RECONCILE_AFTER_S = (config.BANK_REQUEST_TIMEOUT_S + config.BANK_COMMIT_GRACE_S + config.BANK_SEARCH_LAG_S
                     + config.BANK_SEARCH_MARGIN_S)


class LeaseExpired(Exception):
    pass


class Executor:
    def __init__(self, pool: AsyncConnectionPool, r: aioredis.Redis, bank: Bank):
        self.pool, self.bank = pool, bank
        self.post_rate = SlidingWindow(r, "bank:rate:post", config.BANK_RATE_LIMIT)
        self.search_rate = SlidingWindow(r, "bank:rate:search", config.BANK_SEARCH_RATE_LIMIT)
        self.slots = AccountSlots(r, config.BANK_INFLIGHT_PER_ACCOUNT, int((config.BANK_REQUEST_TIMEOUT_S + 2) * 1000))
        self.tokens = TokenCache(r, bank.mint_token)
        self.busy: set[str] = set()
        self.wake = asyncio.Event()
        self.stop = asyncio.Event()

    # -- db helpers ------------------------------------------------------------------------------
    async def x(self, sql, args=()):
        async with self.pool.connection() as c:
            cur = await c.execute(sql, args)
            return cur

    async def claim(self, n: int) -> list[tuple]:
        cur = await self.x(
            "UPDATE payouts p SET lease_owner = %s, lease_until = now() + make_interval(secs => %s) "
            "FROM (SELECT payout_id FROM payouts WHERE status IN ('pending', 'sending', 'unknown') "
            "      AND next_attempt_at <= now() AND (lease_until IS NULL OR lease_until < now()) "
            "      ORDER BY next_attempt_at LIMIT %s FOR UPDATE SKIP LOCKED) d "
            "WHERE p.payout_id = d.payout_id "
            "RETURNING p.payout_id, p.status, p.account, p.currency, p.amount, p.key_gen, "
            "  extract(epoch from (now() - p.attempt_started_at)), p.attempts",
            (ME, config.PAYOUT_LEASE_S, n))
        return await cur.fetchall()

    async def release(self, pid: str, status: str, delay: float, **cols):
        sets = ", ".join(f"{k} = %({k})s" for k in cols)
        await self.x(
            f"UPDATE payouts SET status = %(status)s, next_attempt_at = now() + make_interval(secs => %(delay)s), "
            f"lease_owner = NULL, lease_until = NULL, updated_at = now(){', ' + sets if sets else ''} "
            f"WHERE payout_id = %(pid)s AND lease_owner = %(me)s",
            {"status": status, "delay": delay, "pid": pid, "me": ME, **cols})

    async def record(self, pid, kind, key, label, status):
        try:
            await self.x("INSERT INTO payout_attempts (payout_id, kind, idem_key, outcome, http_status, executor) "
                         "VALUES (%s, %s, %s, %s, %s, %s)", (pid, kind, key, label, status, ME))
        except psycopg.Error:
            pass

    # -- main loop -------------------------------------------------------------------------------
    async def run(self):
        asyncio.create_task(self.listen())
        while not self.stop.is_set():
            free = config.PAYOUT_WORKERS - len(self.busy)
            rows = []
            if free > 0:
                try:
                    rows = await self.claim(free)
                except (psycopg.Error, OSError) as e:
                    log.warning("claim failed", extra={"error": str(e)[:200]})
                    await asyncio.sleep(1)
            for row in rows:
                self.busy.add(row[0])
                asyncio.create_task(self.work(row, time.monotonic()))
            if not rows:
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), 0.5)
                except asyncio.TimeoutError:
                    pass

    async def listen(self):
        """LISTEN payouts: the consumer NOTIFYs when it inserts payouts (polling is the fallback)."""
        while not self.stop.is_set():
            try:
                conn = await db.aconnect(app="payouts-listen")
                await conn.execute("LISTEN payouts")
                async for _ in conn.notifies():
                    self.wake.set()
            except Exception:
                await asyncio.sleep(1)

    async def work(self, row, claimed_at: float):
        pid, status, account, currency, amount, key_gen, since_attempt, attempts = row
        p = {"payout_id": pid, "account": account, "currency": currency.strip() if currency else currency,
             "amount": amount, "key_gen": key_gen, "attempts": attempts}
        deadline = claimed_at + config.PAYOUT_LEASE_S - config.BANK_REQUEST_TIMEOUT_S - 5
        try:
            if status in ("sending", "unknown"):
                done = await self.reconcile(p, status, since_attempt, deadline)
                if done:
                    return
                p["key_gen"] += 1                    # confirmed not executed: fresh key, send again
            await self.send(p, deadline)
        except LeaseExpired:
            log.warning("payout lease ran out before sending; leaving it", extra={"payout_id": pid})
        except (psycopg.Error, OSError) as e:
            log.warning("payout step failed (db); lease will expire and it will be picked up again",
                        extra={"payout_id": pid, "error": str(e)[:200]})
        except Exception:
            log.exception("payout worker crashed", extra={"payout_id": pid})
        finally:
            self.busy.discard(pid)

    # -- POST ------------------------------------------------------------------------------------
    async def send(self, p: dict, deadline: float):
        pid, account = p["payout_id"], p["account"]
        slot = await self.slots.try_acquire(account)
        if slot is None:                              # account busy across the fleet
            await self.release(pid, "pending", 0.5, key_gen=p["key_gen"])
            return
        try:
            if not await self.post_rate.acquire(max_wait=max(0.0, deadline - time.monotonic())):
                await self.release(pid, "pending", 0, key_gen=p["key_gen"])
                return
            token = await self.tokens.get()
            if time.monotonic() > deadline:
                raise LeaseExpired()
            key = f"{pid}#{p['key_gen']}"
            # write-ahead: from here on, nobody may assume this payout was not sent
            cur = await self.x(
                "UPDATE payouts SET status = 'sending', key_gen = %s, attempts = attempts + 1, "
                "attempt_started_at = now(), updated_at = now() "
                "WHERE payout_id = %s AND lease_owner = %s AND lease_until > now() + make_interval(secs => %s)",
                (p["key_gen"], pid, ME, config.BANK_REQUEST_TIMEOUT_S + 2))
            if cur.rowcount != 1:
                raise LeaseExpired()
            body = {"reference": pid, "account": account, "amount_minor": p["amount"], "currency": p["currency"]}
            out: Outcome = await self.bank.post_payout(token, key, body)
        finally:
            await self.slots.release(account, slot)
        await self.record(pid, "post", key, out.label, out.http_status)
        extra = {"payout_id": pid, "idem_key": key, "outcome": out.label, "http_status": out.http_status}
        if out.kind is Kind.PAID:
            await self.release(pid, "paid", 0, bank_payout_id=out.bank_payout_id, reason=None)
            log.info("payout paid", extra=extra)
        elif out.kind is Kind.FAILED:
            await self.release(pid, "failed", 0, reason=out.reason)
            log.info("payout failed permanently", extra=extra)
        elif out.kind is Kind.RETRY:
            if out.token_rejected:
                await self.tokens.invalidate(token)
            await self.release(pid, "pending", out.delay, reason=out.reason)
            log.info("payout not executed, will retry", extra={**extra, "delay_s": out.delay})
        else:
            await self.release(pid, "unknown", RECONCILE_AFTER_S, reason=f"ambiguous:{out.label}")
            log.warning("payout outcome unknown, will reconcile by search", extra=extra)

    # -- reconcile -------------------------------------------------------------------------------
    async def reconcile(self, p: dict, status: str, since_attempt, deadline: float) -> bool:
        """Resolve a `sending`/`unknown` payout from the bank's own record.
        Returns True if the payout reached a final/parked state, False if it must be sent again."""
        pid = p["payout_id"]
        if since_attempt is None:
            since_attempt = RECONCILE_AFTER_S          # never actually started (crash before write-ahead)
        wait = RECONCILE_AFTER_S - float(since_attempt)
        if wait > 0:                                   # a commit of that attempt might not be visible yet
            await self.release(pid, "unknown", wait)
            return True
        if not await self.search_rate.acquire(max_wait=max(0.0, deadline - time.monotonic())):
            await self.release(pid, status, 0.5)
            return True
        token = await self.tokens.get()
        res = await self.bank.search(token, pid)
        if res[0] != "ok":
            if res[0] == "token":
                await self.tokens.invalidate(token)
            await self.record(pid, "search", None, res[2], None)
            await self.release(pid, status, res[1])
            return True
        rows = res[1]
        await self.record(pid, "search", None, f"found_{min(len(rows), 2)}", 200)
        if len(rows) == 0:
            log.info("search: not executed, resending with a new key", extra={"payout_id": pid})
            return False
        if len(rows) > 1:
            log.critical("DOUBLE PAYMENT DETECTED", extra={"payout_id": pid, "bank_rows": [r["payout_id"] for r in rows]})
        await self.release(pid, "paid", 0, bank_payout_id=rows[0]["payout_id"],
                           reason="double_payment_detected" if len(rows) > 1 else None)
        log.info("search: payout was executed", extra={"payout_id": pid, "bank_payout_id": rows[0]["payout_id"]})
        return True


async def main():
    setup_logging("payouts")
    await asyncio.to_thread(db.bootstrap)
    pool = AsyncConnectionPool(config.DATABASE_URL, min_size=2, max_size=config.PAYOUT_WORKERS + 2, open=False,
                               kwargs={"autocommit": True, "application_name": "payouts"}, timeout=10,
                               reconnect_timeout=0)
    await pool.open(wait=False)
    r = aioredis.from_url(config.REDIS_URL, socket_timeout=2, socket_connect_timeout=2)
    ex = Executor(pool, r, Bank())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, ex.stop.set)
    log.info("payout executor up", extra={"reconcile_after_s": RECONCILE_AFTER_S, "workers": config.PAYOUT_WORKERS})
    runner = asyncio.create_task(ex.run())
    await ex.stop.wait()
    runner.cancel()
    for _ in range(100):                               # let in-flight POSTs finish and record their outcome
        if not ex.busy:
            break
        await asyncio.sleep(0.1)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
