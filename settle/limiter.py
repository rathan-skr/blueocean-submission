"""Cross-process coordination for the bank's shared limits, in Redis.

Redis is an optimisation here, never a correctness dependency: if it is wiped or down, every
primitive degrades to "allow" and the bank's own enforcement answers 429 / 409 too_many_inflight,
both of which are definitive "no money moved" responses that the executor simply retries.

  * SlidingWindow  - global requests/second (bank: 20 POST/s, 10 search/s across all callers)
  * AccountSlots   - at most N in-flight payouts per destination account (bank: 2)
  * TokenCache     - the single live OAuth token shared by every executor (issuing a new one
                     revokes the old, and issuance is throttled to 1 per 5 s)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid

import redis.asyncio as aioredis

log = logging.getLogger("limiter")

# KEYS[1]=window key; ARGV: limit, window_ms, member. Returns 0 if admitted, else ms to wait.
_WINDOW_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local win = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - win)
local n = redis.call('ZCARD', KEYS[1])
if n < tonumber(ARGV[1]) then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], win * 2)
  return 0
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return math.max(1, tonumber(oldest[2]) + win - now)
"""

# KEYS[1]=slots key; ARGV: limit, ttl_ms, member. Returns 1 if a slot was taken.
_SLOT_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[1]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[3])
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2)
  return 1
end
return 0
"""

# delete the shared token only if it is still the one the caller saw rejected
_CAS_DEL_LUA = """
local v = redis.call('GET', KEYS[1])
if v and string.find(v, ARGV[1], 1, true) then return redis.call('DEL', KEYS[1]) end
return 0
"""


class SlidingWindow:
    def __init__(self, r: aioredis.Redis, key: str, limit: int, window_ms: int = 1000):
        self.r, self.key, self.limit, self.window_ms = r, key, limit, window_ms
        self._script = r.register_script(_WINDOW_LUA)

    async def acquire(self, max_wait: float = 30.0) -> bool:
        end = time.monotonic() + max_wait
        while time.monotonic() < end:
            try:
                wait_ms = await self._script(keys=[self.key], args=[self.limit, self.window_ms, uuid.uuid4().hex])
            except Exception as e:                      # Redis gone: fail open, the bank enforces anyway
                log.warning("rate limiter unavailable, failing open", extra={"error": repr(e)[:120]})
                await asyncio.sleep(1.0 / max(1, self.limit))
                return True
            if wait_ms == 0:
                return True
            await asyncio.sleep(min(int(wait_ms), 1000) / 1000)
        return False


class AccountSlots:
    def __init__(self, r: aioredis.Redis, limit: int, ttl_ms: int):
        self.r, self.limit, self.ttl_ms = r, limit, ttl_ms
        self._script = r.register_script(_SLOT_LUA)

    async def try_acquire(self, account: str) -> str | None:
        member = uuid.uuid4().hex
        try:
            ok = await self._script(keys=[f"bank:inflight:{account}"], args=[self.limit, self.ttl_ms, member])
        except Exception:
            return member                               # fail open: bank answers 409 too_many_inflight
        return member if ok == 1 else None

    async def release(self, account: str, member: str):
        try:
            await self.r.zrem(f"bank:inflight:{account}", member)
        except Exception:
            pass                                        # the slot's TTL frees it


class TokenCache:
    """One shared bearer token. Only the holder of a short Redis lock may mint a new one."""
    KEY, LOCK = "bank:token", "bank:token:lock"

    def __init__(self, r: aioredis.Redis, mint):
        self.r, self.mint = r, mint          # mint() -> (token, expires_in) | ("retry", seconds)
        self._local: tuple[str, float] | None = None
        self._cas_del = r.register_script(_CAS_DEL_LUA)

    async def get(self) -> str:
        while True:
            try:
                raw = await self.r.get(self.KEY)
                if raw:
                    tok, exp = json.loads(raw)
                    if exp - time.time() > 5:
                        return tok
                got_lock = await self.r.set(self.LOCK, "1", nx=True, px=8000)
            except Exception:
                return await self._get_local()          # Redis down: per-process token (slower, still safe)
            if got_lock:
                try:
                    res = await self.mint()
                    if res[0] == "retry":
                        await asyncio.sleep(res[1])
                        continue
                    tok, ttl = res
                    await self.r.set(self.KEY, json.dumps([tok, time.time() + ttl]), ex=max(1, int(ttl) - 2))
                    return tok
                finally:
                    try:
                        await self.r.delete(self.LOCK)
                    except Exception:
                        pass
            await asyncio.sleep(0.2)                    # someone else is minting

    async def _get_local(self) -> str:
        while True:
            if self._local and self._local[1] - time.time() > 5:
                return self._local[0]
            res = await self.mint()
            if res[0] == "retry":
                await asyncio.sleep(res[1])
                continue
            self._local = (res[0], time.time() + res[1])
            return res[0]

    async def invalidate(self, tok: str):
        """Drop `tok` only if it is still the shared one (someone may already have replaced it)."""
        if self._local and self._local[0] == tok:
            self._local = None
        try:
            await self._cas_del(keys=[self.KEY], args=[tok])
        except Exception:
            pass
