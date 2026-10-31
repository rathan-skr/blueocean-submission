"""R4 - balances drift on redelivery. The README says "ledger consumers are idempotent
(processed_events)", but incumbent/ledger_consumer.py:57 only INSERTs into processed_events -
nothing ever reads it - and does so outside (before) the effect transaction. Kafka redelivers
after every rebalance (the rolling restart), and offsets are committed asynchronously and out of
order from 16 threads (:108, :127), so redelivery is routine. A redelivered CAPTURE is applied
twice: settled money is created from nothing.

Property: delivering the same event twice leaves the same balance as delivering it once.
"""
import psycopg
import pytest

from tests.incumbent.conftest import IMPLS, load
from tests.incumbent.test_r2_outbox_skip import incumbent_db
from tests.pg import fresh_db

M = "M-0042"
EVENTS = [
    {"event_id": f"{M}:1", "merchant_id": M, "type": "MERCHANT_CONFIG", "event_time": "2026-11-01T00:00:01.000Z",
     "currency": "USD", "reserve_minor": 0, "bank_account": "ACCT-1"},
    {"event_id": f"{M}:2", "merchant_id": M, "type": "AUTH", "event_time": "2026-11-01T00:00:02.000Z",
     "currency": "USD", "amount_minor": 10_000},
    {"event_id": f"{M}:3", "merchant_id": M, "type": "CAPTURE", "event_time": "2026-11-01T00:00:03.000Z",
     "currency": "USD", "ref": f"{M}:2", "amount_minor": 10_000, "fee_minor": 320},
]
EXPECTED_SETTLED = 10_000 - 320


def run_incumbent(deliveries):
    url = incumbent_db()
    mod = load("ledger_consumer", DATABASE_URL=url)
    for ev in deliveries:
        mod.apply(ev)
    with psycopg.connect(url) as c:
        v = c.execute("SELECT coalesce(sum(amount), 0) FROM ledger_entries WHERE merchant_id = %s AND bucket = 'settled'",
                      (M,)).fetchone()[0]
    return round(v * 100)


def run_fixed(deliveries):
    from settle import ledger
    url = fresh_db()
    with psycopg.connect(url) as c:
        for ev in deliveries:
            with c.transaction():
                ledger.apply_batch(c.cursor(), [{"type": "DATA", "channel": M, "seq": int(ev["event_id"].split(":")[1]),
                                                 "event": ev}])
        return c.execute("SELECT settled FROM merchants WHERE merchant_id = %s", (M,)).fetchone()[0]


@pytest.mark.parametrize("impl", IMPLS)
def test_redelivered_capture_is_applied_once(impl):
    deliveries = EVENTS + [EVENTS[2]]                  # the CAPTURE comes again after a rebalance
    settled = (run_incumbent if impl == "incumbent" else run_fixed)(deliveries)
    assert settled == EXPECTED_SETTLED
