"""The cross-process limiter: several OS processes share one Redis-backed limit."""
import asyncio
import multiprocessing as mp
import time

import pytest
import redis

from tests.pg import TEST_REDIS_URL

LIMIT, PROCS, EACH = 10, 6, 15


def _require_redis():
    try:
        r = redis.Redis.from_url(TEST_REDIS_URL, socket_connect_timeout=2)
        r.flushdb()
    except redis.exceptions.ConnectionError as e:
        pytest.skip(f"no Redis: {e}")


def rate_worker(q):
    import redis.asyncio as aioredis
    from settle.limiter import SlidingWindow

    async def go():
        lim = SlidingWindow(aioredis.from_url(TEST_REDIS_URL), "test:rate", LIMIT)
        out = []
        for _ in range(EACH):
            assert await lim.acquire(max_wait=60)
            out.append(time.time())
        return out
    q.put(asyncio.run(go()))


def slot_worker(q):
    import redis.asyncio as aioredis
    from settle.limiter import AccountSlots

    async def go():
        r = aioredis.from_url(TEST_REDIS_URL)
        slots = AccountSlots(r, 2, ttl_ms=5000)
        spans = []
        got = 0
        while got < 8:
            m = await slots.try_acquire("ACCT-1")
            if m is None:
                await asyncio.sleep(0.005)
                continue
            t0 = time.time()
            await asyncio.sleep(0.02)
            spans.append((t0, time.time()))
            await slots.release("ACCT-1", m)
            got += 1
        return spans
    q.put(asyncio.run(go()))


def run(target):
    q = mp.Queue()
    ps = [mp.Process(target=target, args=(q,)) for _ in range(PROCS)]
    for p in ps:
        p.start()
    res = [q.get(timeout=90) for _ in ps]
    for p in ps:
        p.join()
    return res


def test_global_rate_limit_holds_across_processes():
    _require_redis()
    stamps = sorted(t for r in run(rate_worker) for t in r)
    assert len(stamps) == PROCS * EACH
    # no sliding window ever holds more than LIMIT admissions. Timestamps are taken client-side after the
    # Redis round trip, so allow 50 ms of measurement jitter on the 1 s window.
    j = 0
    for i, t in enumerate(stamps):
        while stamps[j] < t - 0.95:
            j += 1
        assert i - j + 1 <= LIMIT, f"{i - j + 1} admissions within 1s"
    # and it is not over-restrictive: 90 admissions at 10/s take about 8-9 s
    assert stamps[-1] - stamps[0] < 12


def test_per_account_inflight_never_exceeds_two():
    _require_redis()
    spans = [s for r in run(slot_worker) for s in r]
    events = sorted([(a, 1) for a, _ in spans] + [(b, -1) for _, b in spans])
    cur = peak = 0
    for _, d in events:
        cur += d
        peak = max(peak, cur)
    assert peak <= 2


def test_limiter_fails_open_without_redis():
    from settle.limiter import SlidingWindow
    import redis.asyncio as aioredis

    async def go():
        lim = SlidingWindow(aioredis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2), "k", 5)
        return await lim.acquire(max_wait=2)
    assert asyncio.run(go()) is True
