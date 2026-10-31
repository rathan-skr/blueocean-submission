#!/usr/bin/env python3
"""
BOSP/1 gateway: the acquirer-side session server you consume. See PROTOCOL.md for the contract.

It streams ledger events for every merchant over ONE TCP session per client_id, multiplexed
into one channel per merchant, with per-channel sequence numbers, credit-based flow control,
heartbeats, and resend-on-request. It is deliberately unfriendly: it drops and duplicates
frames, disconnects at random (sometimes mid-frame), and kicks an existing session the moment
another one logs on with the same client_id.

Do not modify this file to make your life easier. Read it if the spec is unclear.

    python gateway.py                        # defaults: 200 merchants x 2,500 frames, 1,000 frames/s
    python gateway.py --rate 3000 --events-per-channel 10000
    python gateway.py --chaos 0              # local debugging only

Admin/status endpoint (plain HTTP, read-only):  GET http://localhost:7101/status

Stdlib only (Python 3.11+).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
from collections import OrderedDict

import ledger_events as le

MAX_FRAME = 64 * 1024
MAX_OUTSTANDING_CREDIT = 10_000
CACHE_WINDOW = 4_000


def env(name, default):
    return type(default)(os.environ.get(name, default))


# ----------------------------------------------------------------------------------------
# Channel content (deterministic) and release clock
# ----------------------------------------------------------------------------------------
class Channel:
    def __init__(self, seed: int, merchant_id: str, count: int):
        self.seed, self.id, self.count = seed, merchant_id, count
        self._gen = le.channel_frames(seed, merchant_id, count)
        self.generated = 0
        self.cache: OrderedDict[int, dict] = OrderedDict()
        self._replay = None
        self._replay_pos = 0
        self.released = 0

    def _remember(self, f):
        self.cache[f["seq"]] = f
        while len(self.cache) > CACHE_WINDOW:
            self.cache.popitem(last=False)

    def get(self, seq: int) -> dict:
        if seq in self.cache:
            return self.cache[seq]
        if seq == self.generated + 1:
            f = next(self._gen)
            self.generated = seq
            self._remember(f)
            return f
        if seq > self.generated:
            while self.generated < seq:
                self.get(self.generated + 1)
            return self.cache[seq]
        # rewind beyond the cache window: regenerate deterministically
        if self._replay is None or self._replay_pos >= seq:
            self._replay = le.channel_frames(self.seed, self.id, self.count)
            self._replay_pos = 0
        f = None
        while self._replay_pos < seq:
            f = next(self._replay)
            self._replay_pos += 1
        return f


class Gateway:
    def __init__(self, a):
        self.a = a
        self.ids = le.merchant_ids(a.merchants)
        self.channels = {m: Channel(a.seed, m, a.events_per_channel) for m in self.ids}
        self.weights = le.merchant_weights(a.seed, a.merchants)
        self.rng = random.Random(f"{a.seed}:release")
        self.sessions: dict[str, "Session"] = {}          # client_id -> live session
        self.history: dict[str, dict[str, int]] = {}      # client_id -> channel -> max seq ever written
        self.last_logon: dict[str, float] = {}
        self.epoch = int(time.time())
        self.started = time.time()
        self.total = a.merchants * a.events_per_channel
        self.released_total = 0
        self.finished_at = None

    def release(self, n: int):
        open_ids = [m for m in self.ids if self.channels[m].released < self.channels[m].count]
        if not open_ids:
            return
        weights = [self.weights[self.ids.index(m)] for m in open_ids] if len(open_ids) < len(self.ids) else self.weights
        picks = self.rng.choices(open_ids, weights=weights, k=n)
        touched = set()
        for m in picks:
            ch = self.channels[m]
            if ch.released < ch.count:
                ch.released += 1
                self.released_total += 1
                touched.add(m)
        for s in self.sessions.values():
            s.mark_ready(touched)
        if self.released_total >= self.total and self.finished_at is None:
            self.finished_at = time.time()

    async def release_clock(self):
        a = self.a
        if a.rate <= 0:
            while self.released_total < self.total:
                self.release(10_000)
            return
        tick, carry = 0.02, 0.0
        while self.released_total < self.total:
            elapsed = time.time() - self.started
            rate = a.rate
            if a.burst_every > 0 and elapsed % a.burst_every >= a.burst_every - a.burst_seconds:
                rate *= a.burst_multiplier
            carry += rate * tick
            n = int(carry)
            carry -= n
            if n:
                self.release(n)
            await asyncio.sleep(tick)

    def status(self) -> dict:
        return {
            "protocol": "BOSP/1",
            "seed": self.a.seed,
            "merchants": self.a.merchants,
            "frames_per_channel": self.a.events_per_channel,
            "total_frames": self.total,
            "released_frames": self.released_total,
            "finished": self.released_total >= self.total,
            "finished_at": self.finished_at,
            "uptime_s": round(time.time() - self.started, 1),
            "rate_per_s": self.a.rate,
            "chaos": bool(self.a.chaos),
            "channels": {m: self.channels[m].count for m in self.ids},
            "released_by_channel": {m: self.channels[m].released for m in self.ids},
            "sessions": [{"client_id": c, "session_epoch": s.epoch, "connected_s": round(time.time() - s.started, 1),
                          "frames_written": s.frames_written} for c, s in self.sessions.items()],
        }


# ----------------------------------------------------------------------------------------
# One client session
# ----------------------------------------------------------------------------------------
class Session:
    def __init__(self, gw: Gateway, reader, writer, client_id: str, epoch: int, resume: dict):
        self.gw, self.reader, self.writer = gw, reader, writer
        self.client_id, self.epoch = client_id, epoch
        self.started = time.time()
        self.alive = True
        self.cursor = {m: max(1, int(resume.get(m, 0)) + 1) for m in gw.ids}
        self.credit = {m: 0 for m in gw.ids}
        self.seen = gw.history.setdefault(client_id, {})
        self.ready: set[str] = set()
        self.wake = asyncio.Event()
        self.last_recv = self.last_sent = time.time()
        self.test_req = None
        self.test_req_at = 0.0
        self.frames_written = 0
        self.rng = random.Random()
        self.disconnect_at = time.time() + self.rng.uniform(gw.a.disconnect_min, gw.a.disconnect_max)

    # -- helpers -------------------------------------------------------------------------
    def mark_ready(self, ids):
        hit = False
        for m in ids:
            if self.credit[m] > 0 and self.cursor[m] <= self.gw.channels[m].released:
                self.ready.add(m)
                hit = True
        if hit:
            self.wake.set()

    def write(self, obj: dict):
        self.writer.write(le.frame_bytes(obj))
        self.last_sent = time.time()

    async def close(self, reason: str | None):
        if not self.alive:
            return
        self.alive = False
        if reason:
            try:
                self.write({"type": "LOGOUT", "reason": reason})
                await asyncio.wait_for(self.writer.drain(), 1)
            except Exception:
                pass
        try:
            self.writer.close()
        except Exception:
            pass
        if self.gw.sessions.get(self.client_id) is self:
            del self.gw.sessions[self.client_id]
        self.wake.set()

    # -- frame building ------------------------------------------------------------------
    def next_frame(self, m: str) -> dict | None:
        ch = self.gw.channels[m]
        seq = self.cursor[m]
        if seq > ch.released:
            return None
        seen = self.seen.get(m, 0)
        f = ch.get(seq)
        if f["kind"] == "NOTICE" and seq <= seen:
            end = seq
            while end + 1 <= min(seen, ch.released) and ch.get(end + 1)["kind"] == "NOTICE":
                end += 1
            self.cursor[m] = end + 1
            return {"type": "GAP_FILL", "channel": m, "seq": seq, "new_seq": end + 1, "poss_dup": True}
        self.cursor[m] = seq + 1
        if f["kind"] == "NOTICE":
            return {"type": "NOTICE", "channel": m, "seq": seq, "poss_dup": False, "text": f["text"]}
        return {"type": "DATA", "channel": m, "seq": seq, "poss_dup": seq <= seen, "event": f["event"]}

    async def sender(self):
        a = self.gw.a
        while self.alive:
            if not self.ready:
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), 0.25)
                except asyncio.TimeoutError:
                    pass
                continue
            if a.chaos and self.rng.random() < 0.0005:
                await asyncio.sleep(self.rng.uniform(1.0, 3.0))           # upstream stall
            batch = list(self.ready)
            self.rng.shuffle(batch)
            self.ready.clear()
            written = 0
            for m in batch:
                if not self.alive:
                    return
                sent_here = 0
                while self.credit[m] > 0 and sent_here < 64:
                    f = self.next_frame(m)
                    if f is None:
                        break
                    if a.chaos and f["type"] != "GAP_FILL" and self.rng.random() < a.drop_rate:
                        continue                                         # lost on the wire: no credit consumed
                    if a.chaos and time.time() >= self.disconnect_at:
                        if self.rng.random() < 0.35:
                            raw = le.frame_bytes(f)
                            self.writer.write(raw[: self.rng.randint(1, len(raw) - 1)])
                        await self.close(None)
                        return
                    self.write(f)
                    self.credit[m] -= 1
                    self.seen[m] = max(self.seen.get(m, 0), f["seq"] if f["type"] != "GAP_FILL" else f["new_seq"] - 1)
                    sent_here += 1
                    self.frames_written += 1
                    if a.chaos and f["type"] == "DATA" and self.credit[m] > 0 and self.rng.random() < a.dup_rate:
                        self.write(f)                                    # duplicated on the wire, poss_dup unchanged
                        self.credit[m] -= 1
                        self.frames_written += 1
                if self.credit[m] > 0 and self.cursor[m] <= self.gw.channels[m].released:
                    self.ready.add(m)
                written += sent_here
                if written >= 256:
                    await self.writer.drain()
                    written = 0
            await self.writer.drain()

    async def heartbeats(self):
        hb = self.gw.a.heartbeat_ms / 1000
        while self.alive:
            await asyncio.sleep(0.2)
            now = time.time()
            if self.gw.a.chaos and now >= self.disconnect_at:
                await self.close(None)                                   # idle sessions get dropped too
                return
            silent = now - self.last_recv
            if silent > 2.5 * hb or (self.test_req and now - self.test_req_at > 2 * hb):
                await self.close("heartbeat_timeout")
                return
            if silent > 1.5 * hb and self.test_req is None:
                self.test_req, self.test_req_at = f"T{int(now * 1000)}", now
                self.write({"type": "TEST_REQUEST", "test_req_id": self.test_req})
            if now - self.last_sent > hb:
                self.write({"type": "HEARTBEAT"})
            try:
                await self.writer.drain()
            except Exception:
                await self.close(None)
                return

    async def reader_loop(self):
        while self.alive:
            try:
                msg = await read_frame(self.reader)
            except Exception:
                await self.close(None)
                return
            if msg is None:
                await self.close(None)
                return
            self.last_recv = time.time()
            t = msg.get("type")
            if t == "HEARTBEAT":
                if msg.get("test_req_id") and msg.get("test_req_id") == self.test_req:
                    self.test_req = None      # only an echo of the id answers a TEST_REQUEST
            elif t == "CREDIT":
                m, n = msg.get("channel"), msg.get("n")
                if m not in self.credit or not isinstance(n, int) or n <= 0:
                    await self.close("protocol_error")
                    return
                self.credit[m] += n
                if self.credit[m] > MAX_OUTSTANDING_CREDIT:
                    await self.close("flow_control_violation")
                    return
                self.mark_ready([m])
            elif t == "RESEND":
                m, frm = msg.get("channel"), msg.get("from_seq")
                if m not in self.cursor or not isinstance(frm, int) or frm < 1:
                    await self.close("protocol_error")
                    return
                self.cursor[m] = min(frm, self.gw.channels[m].count + 1)
                self.mark_ready([m])
            elif t == "LOGOUT":
                await self.close("client_logout")
                return
            else:
                await self.close("protocol_error")
                return


async def read_frame(reader) -> dict | None:
    try:
        head = await reader.readexactly(4)
    except asyncio.IncompleteReadError:
        return None
    n = int.from_bytes(head, "big")
    if n <= 0 or n > MAX_FRAME:
        raise ValueError("bad frame length")
    body = await reader.readexactly(n)
    return json.loads(body)


async def handle_client(gw: Gateway, reader, writer):
    async def reject(reason, **extra):
        writer.write(le.frame_bytes({"type": "LOGON_REJECT", "reason": reason, **extra}))
        try:
            await writer.drain()
        finally:
            writer.close()

    try:
        msg = await asyncio.wait_for(read_frame(reader), 5)
    except Exception:
        writer.close()
        return
    if not msg or msg.get("type") != "LOGON":
        return await reject("expected_logon")
    cid, secret = msg.get("client_id"), msg.get("secret")
    if not isinstance(cid, str) or not cid:
        return await reject("bad_client_id")
    if secret != gw.a.secret:
        return await reject("bad_credentials")
    now = time.time()
    if now - gw.last_logon.get(cid, 0) < 1.0:
        return await reject("throttled", retry_after_ms=1000)
    gw.last_logon[cid] = now
    resume = msg.get("resume") or {}
    if not isinstance(resume, dict):
        return await reject("bad_resume")

    old = gw.sessions.get(cid)
    gw.epoch += 1
    s = Session(gw, reader, writer, cid, gw.epoch, resume)
    gw.sessions[cid] = s
    if old:
        await old.close("superseded")
    s.write({"type": "LOGON_ACK", "session_epoch": s.epoch, "heartbeat_ms": gw.a.heartbeat_ms,
             "channels": gw.ids, "max_outstanding_credit": MAX_OUTSTANDING_CREDIT,
             "server_time": le.iso(le.datetime.now(le.timezone.utc))})
    await writer.drain()
    tasks = [asyncio.create_task(x) for x in (s.reader_loop(), s.sender(), s.heartbeats())]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    await s.close(None)
    for t in tasks:
        t.cancel()


async def handle_admin(gw: Gateway, reader, writer):
    try:
        line = (await asyncio.wait_for(reader.readline(), 5)).decode(errors="replace")
        while (await reader.readline()) not in (b"\r\n", b"\n", b""):
            pass
        path = line.split(" ")[1] if " " in line else "/"
        if path.startswith("/status"):
            body, code = json.dumps(gw.status()).encode(), "200 OK"
        elif path.startswith("/healthz"):
            body, code = b'{"status":"ok"}', "200 OK"
        else:
            body, code = b'{"error":"not_found"}', "404 Not Found"
        writer.write(f"HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                     f"Connection: close\r\n\r\n".encode() + body)
        await writer.drain()
    finally:
        writer.close()


async def main():
    p = argparse.ArgumentParser(description="BOSP/1 gateway")
    p.add_argument("--host", default=env("GATEWAY_HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=env("GATEWAY_PORT", 7100))
    p.add_argument("--admin-port", type=int, default=env("GATEWAY_ADMIN_PORT", 7101))
    p.add_argument("--seed", type=int, default=env("GATEWAY_SEED", 42))
    p.add_argument("--merchants", type=int, default=env("GATEWAY_MERCHANTS", 200))
    p.add_argument("--events-per-channel", type=int, default=env("GATEWAY_EVENTS_PER_CHANNEL", 2500))
    p.add_argument("--rate", type=float, default=env("GATEWAY_RATE", 1000.0), help="frames/s released; 0 = all at once")
    p.add_argument("--burst-every", type=float, default=env("GATEWAY_BURST_EVERY", 90.0))
    p.add_argument("--burst-seconds", type=float, default=env("GATEWAY_BURST_SECONDS", 10.0))
    p.add_argument("--burst-multiplier", type=float, default=env("GATEWAY_BURST_MULTIPLIER", 4.0))
    p.add_argument("--heartbeat-ms", type=int, default=env("GATEWAY_HEARTBEAT_MS", 2000))
    p.add_argument("--secret", default=env("GATEWAY_SECRET", "bo-dev-secret"))
    p.add_argument("--chaos", type=int, default=env("GATEWAY_CHAOS", 1))
    p.add_argument("--drop-rate", type=float, default=env("GATEWAY_DROP_RATE", 0.002))
    p.add_argument("--dup-rate", type=float, default=env("GATEWAY_DUP_RATE", 0.001))
    p.add_argument("--disconnect-min", type=float, default=env("GATEWAY_DISCONNECT_MIN_S", 60.0))
    p.add_argument("--disconnect-max", type=float, default=env("GATEWAY_DISCONNECT_MAX_S", 180.0))
    a = p.parse_args()
    gw = Gateway(a)
    srv = await asyncio.start_server(lambda r, w: handle_client(gw, r, w), a.host, a.port, limit=MAX_FRAME * 2)
    adm = await asyncio.start_server(lambda r, w: handle_admin(gw, r, w), a.host, a.admin_port)
    print(f"BOSP/1 gateway on {a.host}:{a.port} (admin :{a.admin_port}) merchants={a.merchants} "
          f"frames/channel={a.events_per_channel} rate={a.rate}/s chaos={a.chaos}", flush=True)
    asyncio.create_task(gw.release_clock())
    async with srv, adm:
        await asyncio.gather(srv.serve_forever(), adm.serve_forever())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
