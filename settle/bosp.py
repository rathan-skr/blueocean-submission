"""BOSP/1 protocol pieces that are pure and deterministic, so they can be unit-tested without a
socket or a clock: frame codec, the per-channel sequencing state machine, and credit accounting.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

MAX_FRAME = 65_536

ACCEPT, DUP, GAP = "accept", "dup", "gap"


class FrameError(Exception):
    pass


def encode(obj: dict) -> bytes:
    body = json.dumps(obj, separators=(",", ":")).encode()
    return len(body).to_bytes(4, "big") + body


class FrameDecoder:
    """Reassembles length-prefixed frames from arbitrary TCP chunks.

    A chunk may hold part of a frame, several frames, or split anywhere - including inside the
    4-byte length prefix. `reset()` discards a partial frame when the connection drops.
    """

    def __init__(self):
        self._buf = bytearray()

    def feed(self, chunk: bytes) -> list[bytes]:
        self._buf += chunk
        out = []
        while True:
            if len(self._buf) < 4:
                return out
            n = int.from_bytes(self._buf[:4], "big")
            # A length outside 1..64 KiB means the stream is corrupt: the caller must drop the connection.
            if n <= 0 or n > MAX_FRAME:
                raise FrameError(f"bad frame length {n}")
            # Body not complete yet: keep the bytes and wait for the next chunk.
            if len(self._buf) < 4 + n:
                return out
            out.append(bytes(self._buf[4:4 + n]))
            del self._buf[:4 + n]

    def reset(self):
        self._buf.clear()

    @property
    def buffered(self) -> int:
        return len(self._buf)


@dataclass
class ChannelSequencer:
    """Per-channel receive state for one session.

    `expected` is the next seq we can accept. Frames are accepted strictly in order; anything
    below `expected` is a duplicate (whatever its poss_dup flag says), anything above is a gap.
    The gateway resends go-back-N, so frames after a gap are discarded rather than buffered:
    they will come again after the RESEND.

    Lost resends: a RESEND that has not moved `expected` past its from_seq within
    `resend_timeout` is sent again. Tail loss: when the LAST frames of a channel are dropped no
    later frame reveals the gap, so a channel that has been silent for `idle_probe` gets a
    RESEND from `expected`. That is harmless when nothing was lost: it rewinds the server's
    cursor to where it already is.
    """
    # Next seq we can accept (= last applied + 1 at session start).
    expected: int
    resend_timeout: float = 2.0
    idle_probe: float = 4.0
    # from_seq of the RESEND we are waiting on (None = no gap outstanding), and when we sent it.
    resend_from: int | None = None
    resend_at: float = 0.0
    last_frame_at: float = 0.0
    last_probe_at: float = 0.0
    resends: int = 0
    probes: int = 0
    gaps: int = 0
    dups: int = 0
    highest_seen: int = 0

    def on_frame(self, ftype: str, seq: int, new_seq: int | None, now: float) -> tuple[str, int | None]:
        """Classify one sequenced frame. Returns (verdict, resend_from_or_None)."""
        self.last_frame_at = now
        if ftype == "GAP_FILL":
            # A GAP_FILL stands for the whole range [seq, new_seq - 1]; a DATA/NOTICE frame for just [seq, seq].
            lo, hi = seq, int(new_seq) - 1
        else:
            lo = hi = seq
        self.highest_seen = max(self.highest_seen, hi)
        # Entirely behind us: already have it.
        if hi < self.expected:
            self.dups += 1
            return DUP, None
        # Starts at (or before) the next expected seq: accept, jump past its end.
        if lo <= self.expected:
            self.expected = hi + 1
            if self.resend_from is not None and self.expected > self.resend_from:
                self.resend_from = None             # the gap is filled
            return ACCEPT, None
        # Starts after the next expected seq: something in between was lost on the wire.
        self.gaps += 1
        if self.resend_from is None or now - self.resend_at >= self.resend_timeout:
            return GAP, self._resend(now)
        return GAP, None                            # a resend for this gap is already outstanding

    def tick(self, now: float) -> int | None:
        """Timer-driven: returns a from_seq to RESEND, or None."""
        if self.resend_from is not None:
            if now - self.resend_at >= self.resend_timeout:   # the resend (or its answer) was lost
                return self._resend(now)
            return None
        # Silent channel: maybe its last frames were lost. Probe at most once per idle_probe seconds.
        if now - self.last_frame_at >= self.idle_probe and now - self.last_probe_at >= self.idle_probe:
            self.last_probe_at = now
            self.probes += 1
            return self.expected
        return None

    def _resend(self, now: float) -> int:
        self.resend_from, self.resend_at = self.expected, now
        self.resends += 1
        return self.expected


@dataclass
class CreditLedger:
    """Credit for one channel.

    The server's view of our outstanding credit is `window - unreturned`, where `unreturned`
    counts frames it wrote to us that we have not yet given back. We give a frame's credit back
    only once the frame is settled: durably stored in the broker (PubAck received) or known to
    be redundant (duplicate / beyond a gap). So outstanding credit never exceeds `window`
    (< max_outstanding_credit) and the gateway can never run ahead of what the broker has made
    durable by more than `window` frames per channel.
    """
    window: int
    refill_at: int
    unreturned: int = 0     # received, not yet returned to the server
    settled: int = 0        # received AND settled, not yet returned

    def received(self):
        self.unreturned += 1

    def settle(self, n: int = 1) -> int:
        """Mark n received frames settled; returns credit to send now (0 = batch it up)."""
        self.settled += n
        if self.settled >= self.refill_at:
            give, self.settled = self.settled, 0
            self.unreturned -= give
            return give
        return 0
