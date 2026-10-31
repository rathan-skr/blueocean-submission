"""R5 - concurrent payouts overdraw. The wiki: "Payout funds check runs at REPEATABLE READ so
concurrent payouts can't overdraw." incumbent/ledger_consumer.py:59,90-96: each transaction reads
SUM(ledger_entries) from its snapshot and INSERTs a new row. Two transactions that insert
different rows never conflict under REPEATABLE READ (it only detects write-write conflicts on the
SAME row): classic write skew. With 16 threads per consumer and event_id-keyed partitions
(session_reader.py:61), two payout requests of the same merchant do run concurrently.

Property: with funds for only one of two payouts, at most one is accepted.
"""
import threading

import psycopg
import pytest

from tests.incumbent.conftest import IMPLS, load
from tests.incumbent.test_r2_outbox_skip import incumbent_db
from tests.incumbent.test_r4_redelivery_double_apply import EVENTS, M
from tests.pg import fresh_db

# settled after EVENTS = 9_680; two payouts of 6_000 each: only one can be funded
P1 = {"event_id": f"{M}:4", "merchant_id": M, "type": "PAYOUT_REQUEST", "event_time": "2026-11-01T00:00:04.000Z",
      "currency": "USD", "amount_minor": 6_000}
P2 = {**P1, "event_id": f"{M}:5", "event_time": "2026-11-01T00:00:05.000Z"}


def run_incumbent():
    url = incumbent_db()
    mod = load("ledger_consumer", DATABASE_URL=url)
    for ev in EVENTS:
        mod.apply(ev)
    # Force the interleaving the thread pool produces: both read the balance before either writes.
    barrier = threading.Barrier(2, timeout=10)
    real_available = mod.available

    def available(cur, merchant_id):
        v = real_available(cur, merchant_id)
        barrier.wait()
        return v
    mod.available = available
    ts = [threading.Thread(target=mod.apply, args=(p,)) for p in (P1, P2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(20)
    with psycopg.connect(url) as c:
        return c.execute("SELECT count(*) FROM payouts WHERE status = 'accepted'").fetchone()[0]


def run_fixed():
    from settle import ledger
    url = fresh_db()

    def frame(ev):
        return {"type": "DATA", "channel": M, "seq": int(ev["event_id"].split(":")[1]), "event": ev}
    with psycopg.connect(url) as c:
        with c.transaction():
            ledger.apply_batch(c.cursor(), [frame(e) for e in EVENTS])
    # Same race: two transactions apply the two payouts at the same time.
    t1, t2 = psycopg.connect(url), psycopg.connect(url)
    ledger.apply_batch(t1.cursor(), [frame(P1)])          # holds the merchant row lock, not committed
    done = threading.Event()

    def second():
        ledger.apply_batch(t2.cursor(), [frame(P2)])      # blocks on the row lock, then re-reads
        t2.commit()
        done.set()
    th = threading.Thread(target=second)
    th.start()
    assert not done.wait(1.0)                             # really concurrent: it is waiting on t1
    t1.commit()
    th.join(10)
    with psycopg.connect(url) as c:
        return c.execute("SELECT count(*) FROM payouts WHERE status = 'pending'").fetchone()[0]


@pytest.mark.parametrize("impl", IMPLS)
def test_concurrent_payouts_cannot_overdraw(impl):
    accepted = run_incumbent() if impl == "incumbent" else run_fixed()
    assert accepted <= 1
