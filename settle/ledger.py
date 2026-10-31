"""The ledger rules (ASSIGNMENT.md §4) and the exactly-once batch apply.

Two layers:
  * `apply_event()` - pure rules on in-memory state. Unit-tested against the harness reference
    (`ledger_events.apply_event`) on generated streams.
  * `apply_batch()` - loads the state a batch touches with row locks, runs the frames through
    `apply_event()` under the per-channel sequence guard, and writes every effect back in the
    caller's transaction. The caller commits the broker offset in that same transaction.

The sequence guard is what makes the effect exactly-once regardless of how often a frame is
delivered: a frame for channel seq n is applied only if merchants.last_seq = n - 1, and
last_seq is advanced in the same transaction as the effects. Duplicates (seq <= last_seq) are
no-ops; a frame from the future (seq > last_seq + 1) is dropped and will be re-published by the
reader, which always resumes from last_seq (see reader.py).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

log = logging.getLogger("ledger")


@dataclass
class Book:
    merchant_id: str
    currency: str | None = None
    reserve: int = 0
    bank_account: str | None = None
    settled: int = 0
    held: int = 0
    fees: int = 0
    last_seq: int = 0

    @property
    def available(self) -> int:
        return self.settled - self.reserve


@dataclass
class Auth:
    merchant_id: str
    amount: int
    open: bool
    new: bool = False
    dirty: bool = False


@dataclass
class Capture:
    merchant_id: str
    amount: int
    refunded: int
    new: bool = False
    dirty: bool = False


@dataclass
class Effects:
    """Everything a batch wants written. Built in memory, flushed once."""
    entries: list = field(default_factory=list)
    payouts: list = field(default_factory=list)


def apply_event(book: Book, ev: dict, auths: dict[str, Auth], captures: dict[str, Capture]) -> tuple[str, dict | None]:
    """Apply one DATA event to `book`. Returns (result, payout_or_None).

    Mirrors harness/ledger_events.apply_event exactly (that function is the spec where prose and
    code disagree). Fees accrue to book.fees; the platform balance is the sum per currency.
    """
    typ = ev.get("type")
    eid = ev["event_id"]

    def own(rec):  # a ref must point at this merchant's own auth/capture
        return rec if rec is not None and rec.merchant_id == book.merchant_id else None

    # MERCHANT_CONFIG: set reserve + bank account. Only the FIRST config fixes the currency.
    if typ == "MERCHANT_CONFIG":
        if not book.currency:
            book.currency = ev["currency"]
        book.reserve = int(ev["reserve_minor"])
        book.bank_account = ev["bank_account"]
        return "applied", None

    # AUTH: money goes on hold; the auth is open until captured or expired.
    if typ == "AUTH":
        amt = int(ev["amount_minor"])
        auths[eid] = Auth(book.merchant_id, amt, True, new=True)
        book.held += amt
        return "applied", None

    # AUTH_EXPIRY: release the hold of an OPEN auth; anything else is rejected (no effect).
    if typ == "AUTH_EXPIRY":
        a = own(auths.get(ev["ref"]))
        if a is None or not a.open:
            return "rejected:auth_not_open", None
        a.open, a.dirty = False, True
        book.held -= a.amount
        return "applied", None

    # CAPTURE: needs an open auth and amount <= auth amount. Releases the WHOLE hold (even when partial),
    # settles amount - fee, and the fee becomes platform revenue (kept per merchant: no global hot row).
    if typ == "CAPTURE":
        a = own(auths.get(ev["ref"]))
        if a is None or not a.open:
            return "rejected:auth_not_open", None
        amt, fee = int(ev["amount_minor"]), int(ev["fee_minor"])
        if amt > a.amount:
            return "rejected:capture_exceeds_auth", None
        a.open, a.dirty = False, True
        book.held -= a.amount                 # the whole hold is released, even on a partial capture
        book.settled += amt - fee
        book.fees += fee
        captures[eid] = Capture(book.merchant_id, amt, 0, new=True)
        return "applied", None

    # REFUND: needs an existing capture, and total refunded may not exceed the captured amount.
    if typ == "REFUND":
        c = own(captures.get(ev["ref"]))
        if c is None:
            return "rejected:capture_not_found", None
        amt = int(ev["amount_minor"])
        if c.refunded + amt > c.amount:
            return "rejected:refund_exceeds_capture", None
        c.refunded += amt
        c.dirty = True
        book.settled -= amt
        return "applied", None

    # CHARGEBACK: forced by the bank - takes amount + fee even if settled goes negative.
    if typ == "CHARGEBACK":
        if own(captures.get(ev["ref"])) is None:
            return "rejected:capture_not_found", None
        amt, fee = int(ev["amount_minor"]), int(ev["fee_minor"])
        book.settled -= amt + fee             # forced: may go negative
        book.fees += fee
        return "applied", None

    # PAYOUT_REQUEST: accepted only if available (settled - reserve) covers it AT THIS POINT in the
    # sequence. The account and currency are snapshotted now: a later MERCHANT_CONFIG must not redirect it.
    if typ == "PAYOUT_REQUEST":
        amt = int(ev["amount_minor"])
        payout = {"payout_id": eid, "merchant_id": book.merchant_id, "account": book.bank_account,
                  "currency": book.currency, "amount": amt}
        if amt <= 0 or book.available < amt:
            return "rejected:insufficient_funds", {**payout, "status": "rejected", "reason": "insufficient_funds"}
        book.settled -= amt
        return "applied", {**payout, "status": "pending", "reason": None}

    # Unknown event types (e.g. LOYALTY_ACCRUAL): ignored for forward compatibility.
    return "ignored", None                    # unknown type: forward compatible, still consumes its seq


# ----------------------------------------------------------------------------------------------
# Batch apply (inside the caller's transaction)
# ----------------------------------------------------------------------------------------------
@dataclass
class BatchStats:
    applied: int = 0          # frames that advanced a channel
    duplicates: int = 0       # seq <= last_seq
    gaps: int = 0             # seq > last_seq + 1 (dropped; will be re-published)


_REF_AUTH = ("AUTH_EXPIRY", "CAPTURE")
_REF_CAPTURE = ("REFUND", "CHARGEBACK")


def apply_batch(cur, frames: list[dict]) -> BatchStats:
    """`frames` are decoded BOSP frames (DATA / NOTICE / GAP_FILL) in broker order."""
    stats = BatchStats()
    if not frames:
        return stats
    mids = sorted({f["channel"] for f in frames})

    # 1. lock every merchant row this batch touches (sorted: consistent lock order)
    # A merchant's very first frame creates its row (all zeros, last_seq 0) so it can be locked like any other.
    cur.execute("INSERT INTO merchants (merchant_id) SELECT unnest(%s::text[]) ON CONFLICT DO NOTHING", (mids,))
    cur.execute("SELECT merchant_id, currency, reserve, bank_account, settled, held, fees, last_seq "
                # FOR UPDATE: a second transaction touching the same merchant waits here, and when it gets the lock
                # it reads the NEW last_seq (READ COMMITTED re-reads after a lock wait), so it sees our frames as dups.
                "FROM merchants WHERE merchant_id = ANY(%s) ORDER BY merchant_id FOR UPDATE", (mids,))
    books = {r[0]: Book(r[0], (r[1] or None), r[2], r[3], r[4], r[5], r[6], r[7]) for r in cur.fetchall()}
    # Remember where each channel started, so step 4 only writes back merchants that actually moved.
    start_seq = {m: b.last_seq for m, b in books.items()}

    # 2. load the auths / captures the batch refers to
    # Load, in two queries, every auth/capture this batch may need. Auths/captures CREATED inside this batch
    # are added to these dicts by apply_event, so a CAPTURE can refer to an AUTH from three frames earlier.
    auth_refs = {f["event"]["ref"] for f in frames if f["type"] == "DATA" and f["event"].get("type") in _REF_AUTH}
    cap_refs = {f["event"]["ref"] for f in frames if f["type"] == "DATA" and f["event"].get("type") in _REF_CAPTURE}
    auths: dict[str, Auth] = {}
    captures: dict[str, Capture] = {}
    if auth_refs:
        cur.execute("SELECT event_id, merchant_id, amount, open FROM auths WHERE event_id = ANY(%s)", (list(auth_refs),))
        auths = {r[0]: Auth(r[1], r[2], r[3]) for r in cur.fetchall()}
    if cap_refs:
        cur.execute("SELECT event_id, merchant_id, amount, refunded FROM captures WHERE event_id = ANY(%s)", (list(cap_refs),))
        captures = {r[0]: Capture(r[1], r[2], r[3]) for r in cur.fetchall()}

    # 3. run the frames through the rules under the sequence guard
    fx = Effects()
    for f in frames:
        b = books[f["channel"]]
        t = f["type"]
        # GAP_FILL covers seqs [seq, new_seq-1] (resent NOTICEs, no ledger effect). Apply it if it starts at or
        # before the next expected seq and reaches beyond what we have.
        if t == "GAP_FILL":
            first, last = f["seq"], f["new_seq"] - 1
            if last <= b.last_seq:
                stats.duplicates += 1
            elif first <= b.last_seq + 1:
                b.last_seq = last
                stats.applied += 1
            else:
                stats.gaps += 1
            continue
        seq = f["seq"]
        # THE GUARD (rule R1): only the very next seq is applied.
        #   seq <= last_seq      -> duplicate (redelivery, reader resumed earlier, zombie leader): skip.
        #   seq >  last_seq + 1  -> something before it is missing: drop it. The reader resumes from last_seq, so
        #                           the missing frame and this one will be published again.
        if seq <= b.last_seq:
            stats.duplicates += 1
            continue
        if seq != b.last_seq + 1:
            stats.gaps += 1
            continue
        # Applied (even if the rule itself rejects the event: a rejected event still consumes its seq).
        b.last_seq = seq
        stats.applied += 1
        if t != "DATA":                       # NOTICE: occupies its seq, no ledger effect
            continue
        ev = f["event"]
        result, payout = apply_event(b, ev, auths, captures)
        # One journal row per DATA event with the balance AFTER it: this is what makes as_of a single lookup.
        fx.entries.append((b.merchant_id, seq, ev["event_id"], str(ev.get("type")), ev["event_time"], result,
                           ev.get("amount_minor"), b.settled, b.held, b.reserve))
        if payout:
            fx.payouts.append(payout)

    # 4. write everything back
    # Nothing above touched the database except reads; now write the whole batch in a few statements.
    # COPY is the fastest bulk insert in PostgreSQL. PK (merchant_id, seq) is a backstop: a double apply would
    # fail the whole transaction instead of corrupting the ledger.
    if fx.entries:
        with cur.copy("COPY ledger_entries (merchant_id, seq, event_id, event_type, event_time, result, amount, "
                      "settled_after, held_after, reserve_after) FROM STDIN") as cp:
            for row in fx.entries:
                cp.write_row(row)
    new_auths = [(k, a.merchant_id, a.amount, a.open) for k, a in auths.items() if a.new]
    if new_auths:
        with cur.copy("COPY auths (event_id, merchant_id, amount, open) FROM STDIN") as cp:
            for row in new_auths:
                cp.write_row(row)
    closed = [k for k, a in auths.items() if a.dirty and not a.new]
    if closed:
        cur.execute("UPDATE auths SET open = false WHERE event_id = ANY(%s)", (closed,))
    new_caps = [(k, c.merchant_id, c.amount, c.refunded) for k, c in captures.items() if c.new]
    if new_caps:
        with cur.copy("COPY captures (event_id, merchant_id, amount, refunded) FROM STDIN") as cp:
            for row in new_caps:
                cp.write_row(row)
    refunds = [(k, c.refunded) for k, c in captures.items() if c.dirty and not c.new]
    if refunds:
        cur.execute("UPDATE captures c SET refunded = v.r FROM unnest(%s::text[], %s::bigint[]) AS v(id, r) "
                    "WHERE c.event_id = v.id", ([k for k, _ in refunds], [r for _, r in refunds]))
    if fx.payouts:
        cur.executemany(
            "INSERT INTO payouts (payout_id, merchant_id, account, currency, amount, status, reason) "
            "VALUES (%(payout_id)s, %(merchant_id)s, %(account)s, %(currency)s, %(amount)s, %(status)s, %(reason)s)",
            fx.payouts)
        # Wakes the payout executors (LISTEN payouts). Delivered only if this transaction commits.
        cur.execute("SELECT pg_notify('payouts', '')")
    # One UPDATE for all changed merchants (unnest of parallel arrays). last_seq moves here, in the same
    # transaction as every effect above - that is the exactly-once guarantee.
    changed = [b for m, b in books.items() if b.last_seq != start_seq[m]]
    if changed:
        cur.execute(
            "UPDATE merchants m SET currency = v.cur, reserve = v.res, bank_account = v.acct, settled = v.s, "
            "held = v.h, fees = v.f, last_seq = v.ls, updated_at = now() "
            "FROM unnest(%s::text[], %s::text[], %s::bigint[], %s::text[], %s::bigint[], %s::bigint[], %s::bigint[], "
            "%s::bigint[]) AS v(id, cur, res, acct, s, h, f, ls) WHERE m.merchant_id = v.id",
            ([b.merchant_id for b in changed], [b.currency for b in changed], [b.reserve for b in changed],
             [b.bank_account for b in changed], [b.settled for b in changed], [b.held for b in changed],
             [b.fees for b in changed], [b.last_seq for b in changed]))
    return stats


def decode(raw: bytes) -> dict:
    return json.loads(raw)
