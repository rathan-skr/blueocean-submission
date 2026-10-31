"""Exactly-once apply under concurrent redelivery, against real PostgreSQL.

Six processes each push the SAME stream of frames for a set of merchants - with duplicates,
out-of-order frames and different batch boundaries - through ledger.apply_batch at the same
time. The per-merchant last_seq guard (checked under a row lock, in the same transaction as the
effects) must leave every merchant exactly where the reference ledger says.
"""
import multiprocessing as mp
import random

import ledger_events as le
import psycopg

from tests.pg import fresh_db

SEED, MERCHANTS, FRAMES = 42, 12, 400


def frames_for(mid):
    out = []
    for f in le.channel_frames(SEED, mid, FRAMES):
        if f["kind"] == "DATA":
            out.append({"type": "DATA", "channel": mid, "seq": f["seq"], "poss_dup": False, "event": f["event"]})
        else:
            out.append({"type": "NOTICE", "channel": mid, "seq": f["seq"], "poss_dup": False, "text": f["text"]})
    return out


def worker(url, wid, q):
    from settle import ledger
    rng = random.Random(wid)
    streams = {f"M-{i:04d}": frames_for(f"M-{i:04d}") for i in range(1, MERCHANTS + 1)}
    # interleave merchants, inject dups and a few out-of-order frames, cut into random batches
    feed = []
    cursors = {m: 0 for m in streams}
    while cursors:
        m = rng.choice(list(cursors))
        f = streams[m][cursors[m]]
        feed.append(f)
        if rng.random() < 0.05:
            feed.append(f)                                       # duplicate
        if rng.random() < 0.02 and cursors[m] + 3 < len(streams[m]):
            feed.append(streams[m][cursors[m] + 3])              # from the future: must be dropped
        cursors[m] += 1
        if cursors[m] == len(streams[m]):
            del cursors[m]
    applied = 0
    with psycopg.connect(url) as conn:
        i = 0
        while i < len(feed):
            n = rng.randint(1, 120)
            while True:
                try:
                    with conn.transaction():
                        st = ledger.apply_batch(conn.cursor(), feed[i:i + n])
                    break
                except psycopg.errors.DeadlockDetected:
                    continue
            applied += st.applied
            i += n
    q.put(applied)


def test_concurrent_redelivery_applies_each_frame_once():
    url = fresh_db()
    q = mp.Queue()
    ps = [mp.Process(target=worker, args=(url, w, q)) for w in range(6)]
    for p in ps:
        p.start()
    for p in ps:
        p.join(120)
        assert p.exitcode == 0
    total_applied = sum(q.get() for _ in ps)

    # every frame advanced its channel exactly once across all six processes ...
    assert total_applied == MERCHANTS * FRAMES
    platform, payouts = {}, {}
    with psycopg.connect(url) as c:
        for i in range(1, MERCHANTS + 1):
            mid = f"M-{i:04d}"
            book = le.MerchantBook(mid)
            for f in le.channel_frames(SEED, mid, FRAMES):
                if f["kind"] == "DATA":
                    le.apply_event(book, f["event"], platform, payouts)
            got = c.execute("SELECT settled, held, settled - reserve, last_seq FROM merchants WHERE merchant_id = %s",
                            (mid,)).fetchone()
            # ... and the balances are the reference balances
            assert got == (book.settled, book.held, book.available, FRAMES), mid
            n = c.execute("SELECT count(*) FROM ledger_entries WHERE merchant_id = %s", (mid,)).fetchone()[0]
            assert n == sum(1 for f in le.channel_frames(SEED, mid, FRAMES) if f["kind"] == "DATA")
        fees = dict(c.execute("SELECT currency, sum(fees) FROM merchants GROUP BY currency").fetchall())
        assert {k.strip(): v for k, v in fees.items() if v} == {k: v for k, v in platform.items() if v}
        got_payouts = dict(c.execute("SELECT payout_id, status FROM payouts").fetchall())
        assert got_payouts == {k: ("rejected" if v["status"] == "rejected" else "pending") for k, v in payouts.items()}
