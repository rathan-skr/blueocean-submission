"""R3 - two session readers both believe they are leader. incumbent/leader.py:30-37: the lock lives
only in Redis; renewal is a blind PEXPIRE that never checks it still owns the key; `is_leader` is
set once and never re-checked (session_reader.py:83). A Redis restart without persistence (which
happened during the incident) drops the key, the standby acquires it, and the old leader carries
on. Same after a GC pause/SIGSTOP longer than the TTL.

Property: at most one replica considers itself leader at any time.
"""
import time

import pytest
import redis

from tests.incumbent.conftest import IMPLS, load
from tests.pg import TEST_REDIS_URL, fresh_db


def run_incumbent():
    mod = load("leader")
    r = redis.Redis.from_url(TEST_REDIS_URL)
    r.flushdb()
    a, b = mod.LeaderElector(r), mod.LeaderElector(r)
    a.identity, b.identity = "reader-a", "reader-b"
    assert a.try_acquire()
    r.flushdb()                       # Redis node replaced, no persistence
    b.try_acquire()
    r.pexpire(mod.LOCK_KEY, mod.LOCK_TTL_MS)   # a's renew loop (leader.py:37) extends b's lock, unaware
    return [x for x in (a, b) if x.is_leader]


def run_fixed():
    import asyncio
    fresh_db()
    from settle import config
    from settle.reader import Lease
    config.READER_LEASE_TTL_S = 2.0

    async def go():
        a, b = Lease("reader-a"), Lease("reader-b")
        assert await a.try_acquire()
        redis.Redis.from_url(TEST_REDIS_URL).flushdb()        # irrelevant to the lease: it is in PostgreSQL
        assert not await b.try_acquire()
        time.sleep(2.5)                                        # a is paused past its TTL ...
        assert await b.try_acquire()                           # ... so b takes over with a higher epoch
        assert b.epoch > a.epoch
        assert not await a.renew()                             # a wakes: renewal is a CAS on (holder, epoch)
        return [x for x in (a, b) if x.valid()]
    return asyncio.run(go())


@pytest.mark.parametrize("impl", IMPLS)
def test_single_leader(impl):
    leaders = run_incumbent() if impl == "incumbent" else run_fixed()
    assert len(leaders) <= 1
