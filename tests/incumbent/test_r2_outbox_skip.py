"""R2 - accepted payouts never sent. incumbent/payouts.py:93-97,110-111 walks payout_outbox by
`id > last_id`. A bigserial id is taken at INSERT time but becomes visible at COMMIT time; with
16 threads x 3 consumers inserting concurrently, a lower id often commits after a higher one has
already been polled - and the watermark has moved past it for good.

Property: a payout accepted by a transaction that commits late is still picked up.
"""
import asyncio
import types

import psycopg
import pytest
import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool

from tests.incumbent.conftest import IMPLS, INC
from tests.pg import ADMIN_URL, TEST_REDIS_URL, fresh_db

# incumbent/payouts.py:93-97, verbatim
INCUMBENT_POLL = ("SELECT o.id, p.payout_id, p.merchant_id, p.amount, m.bank_account, m.currency "
                  "FROM payout_outbox o JOIN payouts p USING (payout_id) JOIN merchants m USING (merchant_id) "
                  "WHERE o.id > %s AND o.id %% %s = %s ORDER BY o.id LIMIT 100")


def incumbent_db():
    with psycopg.connect(ADMIN_URL, autocommit=True) as c:
        c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'incumbent_test'")
        c.execute("DROP DATABASE IF EXISTS incumbent_test")
        c.execute("CREATE DATABASE incumbent_test")
    url = ADMIN_URL.rsplit("/", 1)[0] + "/incumbent_test"
    with psycopg.connect(url, autocommit=True) as c:
        c.execute(open(f"{INC}/schema.sql").read())
    return url


def accept(conn, pid):
    """What incumbent/ledger_consumer.py:94-96 does for an accepted payout."""
    conn.execute("INSERT INTO payouts (payout_id, merchant_id, amount, status) VALUES (%s, 'M-1', 1.0, 'accepted')",
                 (pid,))
    conn.execute("INSERT INTO payout_outbox (payout_id) VALUES (%s)", (pid,))


def run_incumbent():
    url = incumbent_db()
    with psycopg.connect(url, autocommit=True) as c:
        c.execute("INSERT INTO merchants VALUES ('M-1', 'USD', 0, 'ACCT-1')")
    a, b, poller = psycopg.connect(url), psycopg.connect(url), psycopg.connect(url, autocommit=True)
    sent, last_id = [], 0

    def poll():
        nonlocal last_id
        for oid, pid, *_ in poller.execute(INCUMBENT_POLL, (last_id, 1, 0)).fetchall():
            sent.append(pid)
            last_id = oid                                     # payouts.py:110
    accept(a, "P-early")       # gets outbox id 1, commit delayed (busy consumer thread)
    accept(b, "P-late")        # gets outbox id 2
    b.commit()
    poll()                     # sees id 2 -> watermark = 2
    a.commit()
    poll(); poll()             # id 1 is now visible but below the watermark
    return sent


def run_fixed():
    url = fresh_db()
    a, b = psycopg.connect(url), psycopg.connect(url)
    for conn, pid in ((a, "P-early"), (b, "P-late")):
        conn.execute("INSERT INTO payouts (payout_id, merchant_id, account, currency, amount, status) "
                     "VALUES (%s, 'M-1', 'ACCT-1', 'USD', 100, 'pending')", (pid,))
    b.commit()

    async def go():
        from settle.payouts import Executor
        pool = AsyncConnectionPool(url, kwargs={"autocommit": True}, open=False)
        await pool.open()
        ex = Executor(pool, aioredis.from_url(TEST_REDIS_URL), bank=types.SimpleNamespace(mint_token=None))
        try:
            sent = [r[0] for r in await ex.claim(10)]
            a.commit()
            sent += [r[0] for r in await ex.claim(10)]
            return sent
        finally:
            await pool.close()
    return asyncio.run(go())


@pytest.mark.parametrize("impl", IMPLS)
def test_late_committing_payout_is_not_skipped(impl):
    sent = run_incumbent() if impl == "incumbent" else run_fixed()
    assert sorted(sent) == ["P-early", "P-late"]
