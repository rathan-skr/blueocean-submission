"""R1 - double payment (P1). incumbent/payouts.py:71 sends a fresh uuid4 Idempotency-Key on EVERY
attempt, and :74-77 / :85-86 retry after outcomes where the bank may already have executed
(timeout, 502 after commit). Each retry is a brand new payout at the bank.

Reproduced against the real harness bank with every POST committing and then answering 502.
Property: one accepted payout -> exactly one execution in the bank's own ledger.
"""
import asyncio
import threading
import time
import types

import psycopg
import pytest
import redis
import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool

from tests.bankproc import bank, executions
from tests.incumbent.conftest import IMPLS, load
from tests.pg import TEST_REDIS_URL, fresh_db

REF, ACCT = "M-9001:77", "ACCT-55555555"


STOP = threading.Event()


def _fast_sleep(_s):
    """Compress the incumbent's backoff; and end its endless retry loop once the test has looked."""
    if STOP.is_set():
        raise SystemExit
    time.sleep(0.05)


def run_incumbent(url):
    STOP.clear()
    mod = load("payouts", BANK_URL=url, BANK_CLIENT_SECRET="bo-bank-dev-secret", REDIS_URL=TEST_REDIS_URL)
    mod.acquire_rate_slot = lambda limit=20: None                    # not under test
    mod.time = types.SimpleNamespace(sleep=_fast_sleep, time=time.time)   # compress backoff
    t = threading.Thread(target=mod.send_payout, daemon=True,
                         args=({"payout_id": REF, "bank_account": ACCT, "amount": 12.34, "currency": "USD"},))
    t.start()
    time.sleep(4)                                                    # it never stops retrying; look after 4 s
    STOP.set()


def run_fixed(url):
    from settle import config
    config.BANK_URL = url
    dburl = fresh_db()
    redis.Redis.from_url(TEST_REDIS_URL).flushdb()
    with psycopg.connect(dburl, autocommit=True) as c:
        c.execute("INSERT INTO payouts (payout_id, merchant_id, account, currency, amount, status) "
                  "VALUES (%s, 'M-9001', %s, 'USD', 1234, 'pending')", (REF, ACCT))
    from settle import payouts
    from settle.bankapi import Bank
    payouts.RECONCILE_AFTER_S = 5.0          # 502 = the request completed; 3 s search lag + margin

    async def go():
        pool = AsyncConnectionPool(dburl, kwargs={"autocommit": True}, open=False)
        await pool.open()
        ex = payouts.Executor(pool, aioredis.from_url(TEST_REDIS_URL), Bank())
        end = time.time() + 30
        while time.time() < end:
            for row in await ex.claim(5):
                await ex.work(row, time.monotonic())
            cur = await ex.x("SELECT status FROM payouts WHERE payout_id = %s", (REF,))
            if (await cur.fetchone())[0] in ("paid", "failed"):
                break
            await asyncio.sleep(0.5)
        cur = await ex.x("SELECT status FROM payouts WHERE payout_id = %s", (REF,))
        status = (await cur.fetchone())[0]
        await pool.close()
        return status
    assert asyncio.run(go()) == "paid"


@pytest.mark.parametrize("impl", IMPLS)
def test_ambiguous_502_is_paid_exactly_once(impl):
    with bank(BANK_P_FAIL_AFTER_COMMIT=1.0) as url:          # every POST: commit, then 502
        (run_incumbent if impl == "incumbent" else run_fixed)(url)
        assert executions(url, REF) == 1
