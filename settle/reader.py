"""BOSP session reader.

Run >= 2 replicas. Exactly one holds the reader lease (PostgreSQL row, fencing epoch) and with it
the gateway session; the others poll for the lease.

Per session:
  1. resume point = merchants.last_seq for every channel (what is APPLIED, read from the ledger -
     not a separate checkpoint that could disagree with it);
  2. LOGON, then grant CREDIT_WINDOW per channel;
  3. every in-order DATA / NOTICE / GAP_FILL frame is published, raw, to the JetStream subject of
     its merchant's partition; its credit is returned only when JetStream PubAcks it (durable);
  4. duplicates are discarded; gaps trigger go-back-N RESEND, re-sent if it goes unanswered
     (lost resend), plus an idle probe for tail loss.

Fencing: the lease is renewed every READER_LEASE_RENEW_S with a compare-and-set on (holder,
epoch). Before every publish the reader checks a LOCAL monotonic deadline (last successful renewal
+ TTL - 1 s). A process that was SIGSTOPped wakes past its deadline and stops before publishing
anything. Even if it did publish, the frames are byte-identical to what the new leader publishes
(channel content is fixed by (channel, seq)) and the ledger's last_seq guard drops them.
"""
from __future__ import annotations

import asyncio
import collections
import json
import logging
import signal
import time

import redis.asyncio as aioredis

from . import bosp, broker, config, db
from .log import setup as setup_logging
from .partition import partition_of, subject

log = logging.getLogger("reader")


class SessionEnded(Exception):
    pass


class LeaseLost(Exception):
    pass


class Lease:
    """Reader leadership in PostgreSQL. `epoch` is the fencing token."""

    def __init__(self, holder: str = config.INSTANCE_ID):
        self.holder = holder
        self.epoch: int | None = None
        self.deadline = 0.0          # local monotonic time after which we must assume we are NOT leader
        self.conn = None

    async def _conn(self):
        if self.conn is None or self.conn.closed:
            self.conn = await db.aconnect(app="reader-lease")
        return self.conn

    async def try_acquire(self) -> bool:
        t0 = time.monotonic()
        try:
            c = await self._conn()
            cur = await c.execute(
                "UPDATE reader_lease SET holder = %s, epoch = epoch + 1, since = now(), session_epoch = NULL, "
                "expires_at = now() + make_interval(secs => %s) "
                "WHERE name = 'bosp' AND (expires_at < now() OR holder IS NULL) RETURNING epoch",
                (self.holder, config.READER_LEASE_TTL_S))
            row = await cur.fetchone()
        except Exception as e:
            log.warning("lease acquire failed", extra={"error": repr(e)[:200]})
            self.conn = None
            return False
        if row:
            self.epoch = row[0]
            self.deadline = t0 + config.READER_LEASE_TTL_S - 1.0
            return True
        return False

    async def renew(self) -> bool:
        t0 = time.monotonic()
        try:
            c = await self._conn()
            cur = await c.execute(
                "UPDATE reader_lease SET expires_at = now() + make_interval(secs => %s) "
                "WHERE name = 'bosp' AND holder = %s AND epoch = %s",
                (config.READER_LEASE_TTL_S, self.holder, self.epoch))
            ok = cur.rowcount == 1
        except Exception as e:
            log.warning("lease renew failed", extra={"error": repr(e)[:200], "lease_epoch": self.epoch})
            self.conn = None
            return time.monotonic() < self.deadline       # keep going until the local deadline
        if ok:
            self.deadline = t0 + config.READER_LEASE_TTL_S - 1.0
        return ok

    def valid(self) -> bool:
        return self.epoch is not None and time.monotonic() < self.deadline

    async def record_session(self, session_epoch: int):
        try:
            c = await self._conn()
            await c.execute("UPDATE reader_lease SET session_epoch = %s WHERE name = 'bosp' AND holder = %s "
                            "AND epoch = %s", (session_epoch, self.holder, self.epoch))
        except Exception:
            self.conn = None

    async def release(self):
        try:
            c = await self._conn()
            await c.execute("UPDATE reader_lease SET expires_at = now(), holder = NULL WHERE name = 'bosp' "
                            "AND holder = %s AND epoch = %s", (self.holder, self.epoch))
        except Exception:
            pass
        self.epoch = None


class Session:
    def __init__(self, nc, lease: Lease, metrics: dict, n: int):
        self.nc, self.lease, self.metrics = nc, lease, metrics
        self.inbox = f"_INBOX.reader.{config.INSTANCE_ID.replace(':', '-').replace('.', '-')}.{n}"
        self.pending: collections.OrderedDict[int, tuple[str, float]] = collections.OrderedDict()
        self.ack_n = 0
        self.failed: str | None = None
        self.seq: dict[str, bosp.ChannelSequencer] = {}
        self.credit: dict[str, bosp.CreditLedger] = {}
        self.writer = None
        self.session_epoch = None
        self.hb = 2.0
        self.last_sent = self.last_recv = time.monotonic()

    # -- outbound ----------------------------------------------------------------------------
    def send(self, obj: dict):
        self.writer.write(bosp.encode(obj))
        self.last_sent = time.monotonic()

    def give_credit(self, ch: str, n: int):
        if n > 0:
            self.send({"type": "CREDIT", "channel": ch, "n": n})

    # -- broker acks -------------------------------------------------------------------------
    async def on_ack(self, msg):
        try:
            n = int(msg.subject.rsplit(".", 1)[1])
        except ValueError:
            return
        entry = self.pending.pop(n, None)
        if entry is None:
            return
        ok = False
        if msg.data:
            try:
                body = json.loads(msg.data)
                ok = "error" not in body and "seq" in body
            except ValueError:
                ok = False
        if not ok:
            self.failed = f"publish rejected: {msg.data[:120]!r} {msg.headers}"
            return
        ch = entry[0]
        if self.writer is not None and not self.writer.is_closing():
            self.give_credit(ch, self.credit[ch].settle())

    # -- the session -------------------------------------------------------------------------
    async def run(self, resume: dict[str, int]):
        reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(config.GATEWAY_HOST, config.GATEWAY_PORT, limit=1 << 20), 5)
        sub = await self.nc.subscribe(self.inbox + ".*", cb=self.on_ack)
        try:
            self.send({"type": "LOGON", "client_id": config.GATEWAY_CLIENT_ID, "secret": config.GATEWAY_SECRET,
                       "resume": resume})
            await self.writer.drain()
            dec = bosp.FrameDecoder()
            ack = await self._read_one(reader, dec, timeout=5)
            if ack.get("type") == "LOGON_REJECT":
                wait = ack.get("retry_after_ms", 1000) / 1000
                log.warning("logon rejected", extra={"reason": ack.get("reason")})
                await asyncio.sleep(wait)
                raise SessionEnded(f"logon_reject:{ack.get('reason')}")
            if ack.get("type") != "LOGON_ACK":
                raise SessionEnded(f"unexpected {ack.get('type')}")
            self.session_epoch = ack["session_epoch"]
            self.hb = ack["heartbeat_ms"] / 1000
            window = min(config.CREDIT_WINDOW, int(ack["max_outstanding_credit"]))
            now = time.monotonic()
            for ch in ack["channels"]:
                self.seq[ch] = bosp.ChannelSequencer(resume.get(ch, 0) + 1, config.RESEND_TIMEOUT_S,
                                                     config.IDLE_PROBE_S, last_frame_at=now, last_probe_at=now)
                self.credit[ch] = bosp.CreditLedger(window, min(config.CREDIT_REFILL_AT, window))
                self.send({"type": "CREDIT", "channel": ch, "n": window})
            await self.writer.drain()
            await self.lease.record_session(self.session_epoch)
            self.metrics.update(session_epoch=self.session_epoch, session_started=time.time(),
                                lease_epoch=self.lease.epoch)
            log.info("session up", extra={"session_epoch": self.session_epoch, "lease_epoch": self.lease.epoch,
                                          "channels": len(self.seq), "resumed_from": sum(resume.values())})
            timer = asyncio.create_task(self._timers())
            try:
                await self._recv_loop(reader, dec)
            finally:
                timer.cancel()
        finally:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
            if self.writer:
                self.writer.close()

    async def _read_one(self, reader, dec, timeout):
        end = time.monotonic() + timeout
        while True:
            left = end - time.monotonic()
            if left <= 0:
                raise SessionEnded("logon timeout")
            chunk = await asyncio.wait_for(reader.read(65536), left)
            if not chunk:
                raise SessionEnded("closed during logon")
            frames = dec.feed(chunk)
            if frames:
                if len(frames) > 1:  # anything after LOGON_ACK is processed by the main loop
                    self._early = frames[1:]
                return json.loads(frames[0])

    async def _recv_loop(self, reader, dec):
        m = self.metrics
        early = getattr(self, "_early", [])
        if early:
            await self._handle_bodies(early)
        while True:
            if self.failed:
                raise SessionEnded(self.failed)
            chunk = await reader.read(1 << 16)
            if not chunk:
                raise SessionEnded("gateway closed connection")
            self.last_recv = time.monotonic()
            m["bytes_in"] += len(chunk)
            await self._handle_bodies(dec.feed(chunk))

    async def _handle_bodies(self, bodies):
        m = self.metrics
        for raw in bodies:
            f = json.loads(raw)
            t = f.get("type")
            if t in ("DATA", "NOTICE", "GAP_FILL"):
                ch = f["channel"]
                s = self.seq.get(ch)
                if s is None:
                    raise SessionEnded(f"frame for unknown channel {ch}")
                self.credit[ch].received()
                m["frames"][t] += 1
                verdict, resend = s.on_frame(t, f["seq"], f.get("new_seq"), time.monotonic())
                if verdict == bosp.ACCEPT:
                    if not self.lease.valid():
                        raise LeaseLost("local lease deadline passed")
                    self.ack_n += 1
                    self.pending[self.ack_n] = (ch, time.monotonic())
                    await self.nc.publish(subject(partition_of(ch)), raw, reply=f"{self.inbox}.{self.ack_n}")
                    m["published"] += 1
                else:
                    m["dups" if verdict == bosp.DUP else "gap_frames"] += 1
                    self.give_credit(ch, self.credit[ch].settle())
                    if resend is not None:
                        self.send({"type": "RESEND", "channel": ch, "from_seq": resend})
                        m["resends"] += 1
                        log.info("gap, resend requested", extra={"channel": ch, "from_seq": resend, "got_seq": f["seq"],
                                                                 "session_epoch": self.session_epoch})
            elif t == "TEST_REQUEST":
                self.send({"type": "HEARTBEAT", "test_req_id": f.get("test_req_id")})
                m["test_requests"] += 1
            elif t == "HEARTBEAT":
                pass
            elif t == "LOGOUT":
                raise SessionEnded(f"logout:{f.get('reason')}")
            else:
                log.warning("unknown frame type ignored", extra={"type": t})

    async def _timers(self):
        m = self.metrics
        try:
            while True:
                await asyncio.sleep(0.2)
                now = time.monotonic()
                if now - self.last_recv > 2 * self.hb:
                    m["silence_timeouts"] += 1
                    self.writer.close()                     # unblocks the recv loop
                    return
                if not self.lease.valid():
                    self.writer.close()
                    return
                if self.pending:
                    oldest = next(iter(self.pending.values()))[1]
                    if now - oldest > config.PUBACK_TIMEOUT_S:
                        self.failed = "puback timeout"
                        m["puback_timeouts"] += 1
                        self.writer.close()
                        return
                for ch, s in self.seq.items():
                    frm = s.tick(now)
                    if frm is not None:
                        self.send({"type": "RESEND", "channel": ch, "from_seq": frm})
                        m["resends" if s.resend_from is not None else "idle_probes"] += 1
                if now - self.last_sent >= self.hb / 2:
                    self.send({"type": "HEARTBEAT"})
                await self.writer.drain()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("timer loop ended", extra={"error": repr(e)[:200]})
            self.writer.close()

    def channel_stats(self) -> dict:
        return {ch: {"expected": s.expected, "highest_seen": s.highest_seen, "resends": s.resends,
                     "gaps": s.gaps, "dups": s.dups, "probes": s.probes} for ch, s in self.seq.items()}


async def load_resume() -> dict[str, int]:
    conn = await db.aconnect(app="reader-resume")
    try:
        cur = await conn.execute("SELECT merchant_id, last_seq FROM merchants WHERE last_seq > 0")
        return {r[0]: r[1] for r in await cur.fetchall()}
    finally:
        await conn.close()


async def publish_metrics(r, metrics, session_ref, lease):
    """Best-effort operational state in Redis (losing it loses nothing but a few seconds of view)."""
    while True:
        await asyncio.sleep(2)
        try:
            s = session_ref.get("s")
            doc = {k: v for k, v in metrics.items() if k != "frames"}
            doc["frames"] = dict(metrics["frames"])
            doc["instance"] = config.INSTANCE_ID
            doc["leader"] = lease.valid()
            doc["updated"] = time.time()
            if lease.valid():
                await r.set("reader:status", json.dumps(doc), ex=30)
                if s is not None:
                    await r.set("reader:channels", json.dumps(s.channel_stats()), ex=30)
            await r.set(f"reader:instance:{config.INSTANCE_ID}", json.dumps(doc), ex=30)
        except Exception:
            pass


async def main():
    setup_logging("reader")
    await asyncio.to_thread(db.bootstrap)
    nc, js = await broker.connect("reader")
    await broker.ensure_stream(js)
    r = aioredis.from_url(config.REDIS_URL, socket_timeout=2, socket_connect_timeout=2)
    lease = Lease()
    metrics = {"frames": collections.Counter(), "published": 0, "dups": 0, "gap_frames": 0, "resends": 0,
               "idle_probes": 0, "test_requests": 0, "silence_timeouts": 0, "puback_timeouts": 0, "sessions": 0,
               "bytes_in": 0, "session_epoch": None, "lease_epoch": None}
    session_ref: dict = {}
    asyncio.create_task(publish_metrics(r, metrics, session_ref, lease))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async def renewer():
        while lease.epoch is not None:
            await asyncio.sleep(config.READER_LEASE_RENEW_S)
            if not await lease.renew():
                log.warning("lease lost", extra={"lease_epoch": lease.epoch})
                lease.deadline = 0
                return

    n = 0
    while not stop.is_set():
        if not lease.valid():
            lease.epoch = None
            if not await lease.try_acquire():
                await asyncio.sleep(1)
                continue
            log.info("became leader", extra={"lease_epoch": lease.epoch})
            asyncio.create_task(renewer())
        if not nc.is_connected:
            await asyncio.sleep(0.5)
            continue
        n += 1
        metrics["sessions"] += 1
        s = Session(nc, lease, metrics, n)
        session_ref["s"] = s
        task = None
        try:
            resume = await load_resume()
            task = asyncio.create_task(s.run(resume))
            stopper = asyncio.create_task(stop.wait())
            done, _ = await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
            stopper.cancel()
            if task in done:
                task.result()
            else:
                s.send({"type": "LOGOUT"})
                await asyncio.sleep(0.1)
                task.cancel()
        except (SessionEnded, LeaseLost, ConnectionError, OSError, asyncio.TimeoutError, bosp.FrameError,
                asyncio.IncompleteReadError) as e:
            log.info("session ended", extra={"reason": repr(e)[:200], "session_epoch": s.session_epoch,
                                             "lease_epoch": lease.epoch})
            if isinstance(e, SessionEnded) and "superseded" in str(e):
                lease.deadline = 0                       # someone else holds the gateway session
        except Exception:
            log.exception("session crashed", extra={"session_epoch": s.session_epoch})
        finally:
            session_ref.pop("s", None)
        await asyncio.sleep(0.3)
    await lease.release()
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
