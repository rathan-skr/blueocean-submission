"""
Deterministic event model shared by the gateway, the bank simulator and the verifier.

Every merchant is one channel. A channel is an ordered stream of frames numbered 1..N.
Most frames are DATA frames carrying one ledger event; some are NOTICE frames (no ledger
effect, but they still occupy a sequence number). The content of every channel is a pure
function of (seed, merchant count, events per channel) - delivery may be chaotic, content
never is.

Stdlib only. You may import this module from tests or tooling, but your pipeline must get
its events from the gateway over the BOSP protocol, never by regenerating them here.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterator

EPOCH = datetime(2026, 11, 1, tzinfo=timezone.utc)
CURRENCIES = ["USD", "USD", "USD", "EUR", "GBP"]


def merchant_ids(merchants: int) -> list[str]:
    return [f"M-{i:04d}" for i in range(1, merchants + 1)]


def channel_rng(seed: int, merchant_id: str, salt: str = "events") -> random.Random:
    # str seeds are hashed with SHA-512 by random.seed(version=2): stable across runs/platforms.
    return random.Random(f"{seed}:{merchant_id}:{salt}")


def bank_account(seed: int, merchant_id: str) -> str:
    rng = channel_rng(seed, merchant_id, "account")
    return f"ACCT-{rng.randrange(10**7, 10**8)}"


def closed_accounts(seed: int, merchants: int) -> set[str]:
    """~4% of merchants have a closed bank account: every payout to them fails permanently."""
    out = set()
    for mid in merchant_ids(merchants):
        if channel_rng(seed, mid, "closed").random() < 0.04:
            out.add(bank_account(seed, mid))
    return out


def merchant_weights(seed: int, merchants: int) -> list[float]:
    """Zipf-ish traffic share: a few merchants are very hot, most are quiet."""
    ids = merchant_ids(merchants)
    rng = random.Random(f"{seed}:weights")
    ranks = list(range(1, merchants + 1))
    rng.shuffle(ranks)
    return [1.0 / (r ** 0.9) for r in ranks]


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


@dataclass
class _GenState:
    currency: str = "USD"
    reserve: int = 0
    settled: int = 0
    open_auths: dict = field(default_factory=dict)   # event_id -> amount
    closed_auths: list = field(default_factory=list)
    captures: dict = field(default_factory=dict)     # event_id -> [amount, refunded]


def channel_frames(seed: int, merchant_id: str, count: int) -> Iterator[dict]:
    """Yield the `count` frames of one channel, seq 1..count, deterministically.

    Each yielded item is {"seq": n, "kind": "DATA"|"NOTICE", "event": {...}|None, "text": str|None}.
    """
    rng = channel_rng(seed, merchant_id)
    st = _GenState(currency=rng.choice(CURRENCIES))
    t = EPOCH + timedelta(seconds=rng.uniform(0, 3600))
    acct = bank_account(seed, merchant_id)

    for seq in range(1, count + 1):
        t += timedelta(milliseconds=rng.randint(400, 45_000))
        eid = f"{merchant_id}:{seq}"
        base = {"event_id": eid, "merchant_id": merchant_id, "event_time": iso(t), "currency": st.currency}

        if seq == 1:
            st.reserve = rng.choice([0, 0, 5_000, 25_000, 100_000])
            yield {"seq": seq, "kind": "DATA", "text": None, "event": {
                **base, "type": "MERCHANT_CONFIG", "reserve_minor": st.reserve, "bank_account": acct}}
            continue

        r = rng.random()
        if r < 0.05:
            yield {"seq": seq, "kind": "NOTICE", "event": None,
                   "text": rng.choice(["settlement window opened", "risk review cleared",
                                       "statement generated", "kyc refresh scheduled"])}
            continue

        ev: dict
        if r < 0.40 or not st.open_auths:
            amt = rng.randint(500, 250_000)
            st.open_auths[eid] = amt
            ev = {**base, "type": "AUTH", "amount_minor": amt}
        elif r < 0.66:
            ref = rng.choice(list(st.open_auths))
            auth_amt = st.open_auths.pop(ref)
            st.closed_auths.append(ref)
            amt = auth_amt if rng.random() < 0.8 else rng.randint(max(1, auth_amt // 2), auth_amt)
            fee = min(amt, round(amt * 0.029) + 30)
            st.captures[eid] = [amt, 0]
            st.settled += amt - fee
            ev = {**base, "type": "CAPTURE", "ref": ref, "amount_minor": amt, "fee_minor": fee}
        elif r < 0.70:
            ref = rng.choice(list(st.open_auths))
            st.open_auths.pop(ref)
            st.closed_auths.append(ref)
            ev = {**base, "type": "AUTH_EXPIRY", "ref": ref}
        elif r < 0.78 and st.captures:
            ref = rng.choice(list(st.captures))
            cap, refunded = st.captures[ref]
            room = cap - refunded
            if rng.random() < 0.06 or room <= 0:
                amt = room + rng.randint(1, 5_000)            # over-refund: must be rejected
            else:
                amt = rng.randint(1, room)
                st.captures[ref][1] += amt
                st.settled -= amt
            ev = {**base, "type": "REFUND", "ref": ref, "amount_minor": amt}
        elif r < 0.795 and st.captures:
            ref = rng.choice(list(st.captures))
            cap = st.captures[ref][0]
            fee = 1_500
            st.settled -= cap + fee
            ev = {**base, "type": "CHARGEBACK", "ref": ref, "amount_minor": cap, "fee_minor": fee}
        elif r < 0.7985:
            avail = st.settled - st.reserve
            amt = max(100, int(max(avail, 20_000) * rng.uniform(0.25, 1.6)))
            if avail >= amt:
                st.settled -= amt
            ev = {**base, "type": "PAYOUT_REQUEST", "amount_minor": amt}
        elif r < 0.8005 and st.closed_auths:
            ref = rng.choice(st.closed_auths)                  # double capture / capture of expired auth
            amt = rng.randint(500, 50_000)
            ev = {**base, "type": "CAPTURE", "ref": ref, "amount_minor": amt, "fee_minor": round(amt * 0.029) + 30}
        elif r < 0.8010:
            st.reserve = rng.choice([0, 5_000, 25_000, 100_000, 250_000])
            ev = {**base, "type": "MERCHANT_CONFIG", "reserve_minor": st.reserve, "bank_account": acct}
        elif r < 0.8014:
            ev = {**base, "type": "LOYALTY_ACCRUAL", "points": rng.randint(1, 500)}  # a newer event type: must be ignored
        else:
            amt = rng.randint(500, 250_000)
            st.open_auths[eid] = amt
            ev = {**base, "type": "AUTH", "amount_minor": amt}
        yield {"seq": seq, "kind": "DATA", "event": ev, "text": None}


# --------------------------------------------------------------------------------------
# Reference ledger rules (the verifier's ground truth). ASSIGNMENT.md states the same
# rules in prose; if the two ever disagree, this function wins.
# --------------------------------------------------------------------------------------
@dataclass
class MerchantBook:
    merchant_id: str
    currency: str = ""
    reserve: int = 0
    bank_account: str = ""
    settled: int = 0
    held: int = 0
    last_seq: int = 0
    auths: dict = field(default_factory=dict)       # event_id -> [amount, open]
    captures: dict = field(default_factory=dict)    # event_id -> [amount, refunded]

    @property
    def available(self) -> int:
        return self.settled - self.reserve


def apply_event(book: MerchantBook, ev: dict, platform: dict, payouts: dict) -> str:
    """Apply one DATA event. Returns "applied" or "rejected:<reason>" or "ignored"."""
    typ = ev["type"]
    eid = ev["event_id"]
    if typ == "MERCHANT_CONFIG":
        if not book.currency:
            book.currency = ev["currency"]
        book.reserve = ev["reserve_minor"]
        book.bank_account = ev["bank_account"]
        return "applied"
    if typ == "AUTH":
        book.auths[eid] = [ev["amount_minor"], True]
        book.held += ev["amount_minor"]
        return "applied"
    if typ == "AUTH_EXPIRY":
        a = book.auths.get(ev["ref"])
        if not a or not a[1]:
            return "rejected:auth_not_open"
        a[1] = False
        book.held -= a[0]
        return "applied"
    if typ == "CAPTURE":
        a = book.auths.get(ev["ref"])
        if not a or not a[1]:
            return "rejected:auth_not_open"
        if ev["amount_minor"] > a[0]:
            return "rejected:capture_exceeds_auth"
        a[1] = False
        book.held -= a[0]
        book.settled += ev["amount_minor"] - ev["fee_minor"]
        platform[book.currency] = platform.get(book.currency, 0) + ev["fee_minor"]
        book.captures[eid] = [ev["amount_minor"], 0]
        return "applied"
    if typ == "REFUND":
        c = book.captures.get(ev["ref"])
        if not c:
            return "rejected:capture_not_found"
        if c[1] + ev["amount_minor"] > c[0]:
            return "rejected:refund_exceeds_capture"
        c[1] += ev["amount_minor"]
        book.settled -= ev["amount_minor"]
        return "applied"
    if typ == "CHARGEBACK":
        if ev["ref"] not in book.captures:
            return "rejected:capture_not_found"
        book.settled -= ev["amount_minor"] + ev["fee_minor"]
        platform[book.currency] = platform.get(book.currency, 0) + ev["fee_minor"]
        return "applied"
    if typ == "PAYOUT_REQUEST":
        amt = ev["amount_minor"]
        if amt <= 0 or book.available < amt:
            payouts[eid] = {"status": "rejected", "amount_minor": amt, "account": book.bank_account,
                            "currency": book.currency, "merchant_id": book.merchant_id}
            return "rejected:insufficient_funds"
        book.settled -= amt
        payouts[eid] = {"status": "accepted", "amount_minor": amt, "account": book.bank_account,
                        "currency": book.currency, "merchant_id": book.merchant_id}
        return "applied"
    return "ignored"


def frame_bytes(obj: dict) -> bytes:
    body = json.dumps(obj, separators=(",", ":")).encode()
    return len(body).to_bytes(4, "big") + body
