#!/usr/bin/env python3
"""
Verifier: compares what your system says against ground truth.

    python verify.py watch  --api http://localhost:8080     # live backlog / throughput while a run is in progress
    python verify.py check  --api http://localhost:8080     # after the gateway reports finished: full correctness check

`check` recomputes every merchant's ledger from the deterministic event model, then checks:
  1. every channel fully consumed (your last_seq == the channel's final seq)
  2. settled / held / available balances for every merchant, and platform fee totals per currency
  3. point-in-time balances (?as_of=) on sampled timestamps, plus their latency
  4. every payout's status, and the bank's own record of money moved: exactly one execution per
     paid payout, none for rejected/failed ones, no unknown references, amounts/accounts/currency correct

Reviewers run this same script (with a different seed, more data and harsher chaos), so make it pass
for the right reasons. Exit code 0 = all checks passed.

Stdlib only (Python 3.11+).
"""
from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict

import ledger_events as le


def get(url: str, timeout: float = 10.0):
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null"), time.perf_counter() - t
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read() or b"null")
        except Exception:
            body = None
        return e.code, body, time.perf_counter() - t
    except Exception as e:
        return 0, {"error": repr(e)}, time.perf_counter() - t


def gateway_status(a):
    code, body, _ = get(a.gateway.rstrip("/") + "/status")
    if code != 200:
        sys.exit(f"cannot reach gateway admin at {a.gateway}: {code} {body}")
    return body


def balance(a, mid, as_of=None):
    url = f"{a.api.rstrip('/')}/v1/merchants/{mid}/balance"
    if as_of:
        url += "?" + urllib.parse.urlencode({"as_of": as_of})
    return get(url)


def watch(a):
    print("time      released   applied    backlog   applied/s")
    prev = None
    while True:
        st = gateway_status(a)
        rel = st["released_by_channel"]
        applied = 0
        for mid in rel:
            code, b, _ = balance(a, mid)
            applied += (b or {}).get("last_seq", 0) if code == 200 else 0
        now = time.time()
        rate = (applied - prev[1]) / (now - prev[0]) if prev else 0.0
        print(f"{time.strftime('%H:%M:%S')}  {st['released_frames']:>9,} {applied:>9,} {st['released_frames'] - applied:>9,}"
              f"   {rate:>8,.0f}{'   (gateway finished)' if st['finished'] else ''}", flush=True)
        prev = (now, applied)
        if st["finished"] and applied >= st["total_frames"]:
            print("caught up.")
            return
        time.sleep(a.interval)


def expected_state(seed, channels):
    books, platform, payouts = {}, {}, {}
    for mid, final in channels.items():
        book = le.MerchantBook(mid)
        for f in le.channel_frames(seed, mid, final):
            if f["kind"] == "DATA":
                le.apply_event(book, f["event"], platform, payouts)
        book.last_seq = final
        books[mid] = book
    return books, platform, payouts


def check(a):
    st = gateway_status(a)
    if not st["finished"] and not a.partial:
        sys.exit("gateway has not finished releasing frames yet (use `watch`, or --partial to check anyway)")
    if st["seed"] != a.seed:
        print(f"note: gateway seed is {st['seed']}; using it")
        a.seed = st["seed"]
    channels = st["channels"]
    print(f"recomputing ground truth for {len(channels)} channels / {st['total_frames']:,} frames ...", flush=True)
    books, platform, payouts = expected_state(a.seed, channels)
    closed = le.closed_accounts(a.seed, st["merchants"])
    failures = defaultdict(list)

    # 1. drained -----------------------------------------------------------------------
    deadline = time.time() + a.drain_timeout
    while True:
        lagging = []
        got = {}
        for mid, book in books.items():
            code, b, _ = balance(a, mid)
            got[mid] = (code, b)
            if code != 200 or b.get("last_seq") != book.last_seq:
                lagging.append(mid)
        if not lagging or time.time() > deadline:
            break
        print(f"  waiting for {len(lagging)} channels to drain ...", flush=True)
        time.sleep(5)
    for mid in lagging:
        failures["drain"].append(f"{mid}: last_seq={got[mid][1] and got[mid][1].get('last_seq')} expected {books[mid].last_seq}")

    # 2. balances ----------------------------------------------------------------------
    for mid, book in books.items():
        code, b = got[mid]
        if code != 200:
            failures["balance"].append(f"{mid}: HTTP {code}")
            continue
        exp = {"settled_minor": book.settled, "held_minor": book.held, "available_minor": book.available}
        diff = {k: (b.get(k), v) for k, v in exp.items() if b.get(k) != v}
        if diff:
            failures["balance"].append(f"{mid}: {diff}  (got, expected)")
    code, pb, _ = get(a.api.rstrip("/") + "/v1/platform/balances")
    if code != 200:
        failures["platform"].append(f"HTTP {code}")
    else:
        for cur in set(platform) | set(pb or {}):
            if (pb or {}).get(cur, 0) != platform.get(cur, 0):
                failures["platform"].append(f"{cur}: got {(pb or {}).get(cur)} expected {platform.get(cur, 0)}")

    # 3. as_of -------------------------------------------------------------------------
    lat = []
    samples = _collect_samples(a, channels, rng=random.Random(a.sample_seed))
    for mid, ts, s, h, av in samples:
        code, b, dt = balance(a, mid, ts)
        lat.append(dt * 1000)
        if code != 200:
            failures["as_of"].append(f"{mid} @ {ts}: HTTP {code}")
            continue
        exp = {"settled_minor": s, "held_minor": h, "available_minor": av}
        diff = {k: (b.get(k), v) for k, v in exp.items() if b.get(k) != v}
        if diff:
            failures["as_of"].append(f"{mid} @ {ts}: {diff}")

    # 4. payouts -----------------------------------------------------------------------
    want = {}
    for pid, p in payouts.items():
        if p["status"] == "rejected":
            want[pid] = "rejected"
        else:
            want[pid] = "failed" if p["account"] in closed else "paid"
    deadline = time.time() + a.payout_timeout
    while True:
        pending, statuses = [], {}
        for pid in want:
            code, b, _ = get(f"{a.api.rstrip('/')}/v1/payouts/{urllib.parse.quote(pid)}")
            statuses[pid] = (code, b)
            if code != 200 or b.get("status") not in ("paid", "failed", "rejected"):
                pending.append(pid)
        if not pending or time.time() > deadline:
            break
        print(f"  waiting for {len(pending)} payouts to reach a terminal status ...", flush=True)
        time.sleep(10)
    for pid, w in want.items():
        code, b = statuses[pid]
        if code != 200:
            failures["payout_status"].append(f"{pid}: HTTP {code}")
        elif b.get("status") != w:
            failures["payout_status"].append(f"{pid}: status {b.get('status')!r}, expected {w!r}")

    code, bank, _ = get(a.bank.rstrip("/") + "/_admin/payouts", timeout=30)
    if code != 200:
        failures["bank"].append(f"cannot read bank admin ledger: HTTP {code}")
    else:
        by_ref = defaultdict(list)
        for p in bank["payouts"]:
            by_ref[p["reference"]].append(p)
        for ref, rows in by_ref.items():
            if ref not in want:
                failures["bank"].append(f"money moved for unknown reference {ref!r} ({len(rows)}x)")
        for pid, w in want.items():
            rows = by_ref.get(pid, [])
            exp_n = 1 if w == "paid" else 0
            if len(rows) != exp_n:
                failures["bank"].append(f"{pid}: bank executed {len(rows)}x, expected {exp_n}x"
                                        + (" (DOUBLE PAYMENT)" if len(rows) > 1 else ""))
            for r in rows[:1]:
                p = payouts[pid]
                for k in ("amount_minor", "account", "currency"):
                    if r[k] != p[k]:
                        failures["bank"].append(f"{pid}: bank {k}={r[k]!r} expected {p[k]!r}")
        print(f"bank stats: {json.dumps(bank.get('stats', {}))}")

    # report ----------------------------------------------------------------------------
    n_paid = sum(1 for w in want.values() if w == "paid")
    print()
    print(f"merchants: {len(books)}   frames: {st['total_frames']:,}   payouts: {len(want)} "
          f"(paid {n_paid}, failed {sum(1 for w in want.values() if w == 'failed')}, "
          f"rejected {sum(1 for w in want.values() if w == 'rejected')})")
    if lat:
        lat.sort()
        p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))]
        print(f"as_of latency over HTTP: p50 {statistics.median(lat):.1f} ms, p99 {p99:.1f} ms "
              f"({len(lat)} queries; target p99 < {a.as_of_p99_ms} ms){'  << SLOW' if p99 > a.as_of_p99_ms else ''}")
    ok = True
    for section in ("drain", "balance", "platform", "as_of", "payout_status", "bank"):
        rows = failures.get(section, [])
        mark = "PASS" if not rows else f"FAIL ({len(rows)})"
        print(f"  {section:<14} {mark}")
        for r in rows[: a.show]:
            print(f"      - {r}")
        if len(rows) > a.show:
            print(f"      ... {len(rows) - a.show} more")
        ok &= not rows
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def _collect_samples(a, channels, rng):
    out = []
    for mid, final in channels.items():
        book = le.MerchantBook(mid)
        plat, pays = {}, {}
        data = [f for f in le.channel_frames(a.seed, mid, final) if f["kind"] == "DATA"]
        if not data:
            continue
        picks = set(rng.sample(range(len(data)), min(a.samples, len(data))))
        for i, f in enumerate(data):
            le.apply_event(book, f["event"], plat, pays)
            if i in picks:
                out.append((mid, f["event"]["event_time"], book.settled, book.held, book.available))
        out.append((mid, "2026-10-01T00:00:00.000Z", 0, 0, 0))
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=["watch", "check"])
    p.add_argument("--api", default="http://localhost:8080", help="your read API base URL")
    p.add_argument("--gateway", default="http://localhost:7101", help="gateway admin URL")
    p.add_argument("--bank", default="http://localhost:7200", help="bank simulator URL")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--samples", type=int, default=2, help="as_of samples per merchant")
    p.add_argument("--sample-seed", type=int, default=7)
    p.add_argument("--drain-timeout", type=float, default=120)
    p.add_argument("--payout-timeout", type=float, default=900)
    p.add_argument("--as-of-p99-ms", type=float, default=50)
    p.add_argument("--interval", type=float, default=5)
    p.add_argument("--show", type=int, default=10)
    p.add_argument("--partial", action="store_true")
    a = p.parse_args()
    if a.mode == "watch":
        watch(a)
        return 0
    return check(a)


if __name__ == "__main__":
    sys.exit(main())
