"""
Leader election for the BOSP session reader.

Only one reader may hold the gateway session (the gateway kicks the older one), so every
reader replica runs this loop and only the elected leader connects.
"""
import logging
import os
import socket
import threading
import time

import redis

log = logging.getLogger("leader")

LOCK_KEY = "bosp:leader"
LOCK_TTL_MS = 15_000
RENEW_EVERY_S = 10


class LeaderElector:
    def __init__(self, r: redis.Redis):
        self.r = r
        self.identity = f"{socket.gethostname()}:{os.getpid()}"
        self.is_leader = False
        self._stop = threading.Event()

    def try_acquire(self) -> bool:
        ok = self.r.set(LOCK_KEY, self.identity, nx=True, px=LOCK_TTL_MS)
        self.is_leader = bool(ok)
        return self.is_leader

    def _renew_loop(self):
        while not self._stop.wait(RENEW_EVERY_S):
            # keep the lock alive while we are working
            self.r.pexpire(LOCK_KEY, LOCK_TTL_MS)

    def start(self):
        while not self.try_acquire():
            time.sleep(2)
        log.info("became leader as %s", self.identity)
        threading.Thread(target=self._renew_loop, daemon=True).start()

    def release(self):
        self._stop.set()
        self.r.delete(LOCK_KEY)
        self.is_leader = False
        log.info("released leadership")
