"""R7 - an executor "stops doing anything". incumbent/payouts.py:83 has an operator-precedence bug:
`409 or 422 and error != "account_closed"` - so `422 account_closed`, the one permanent failure,
falls through to "retrying" (:85-86) forever, while poll_outbox (:101-107) works rows one at a
time holding pg_advisory_lock. One payout to a closed account freezes the whole replica (its
shard of the outbox then waits until someone restarts it).

Against the real harness bank (accounts of ~4 % of merchants are closed).
Property: a payout to a closed account reaches `failed` promptly and is never retried.
"""
import asyncio
import threading
import time
import types

import ledger_events as le
import pytest

from tests.bankproc import bank
from tests.incumbent.conftest import IMPLS, load
from tests.pg import TEST_REDIS_URL

CLOSED = sorted(le.closed_accounts(42, 200))[0]


STOP = threading.Event()


def _fast_sleep(_s):
    """Compress the incumbent's backoff; and end its endless retry loop once the test has looked."""
    if STOP.is_set():
        raise SystemExit
    time.sleep(0.05)


def _quietly(fn, *args):
    """Run incumbent code in a thread; the SystemExit we use to stop its retry loop is expected."""
    try:
        return fn(*args)
    except SystemExit:
        return None


def run_incumbent(url):
    STOP.clear()
    mod = load("payouts", BANK_URL=url, BANK_CLIENT_SECRET="bo-bank-dev-secret", REDIS_URL=TEST_REDIS_URL)
    mod.acquire_rate_slot = lambda limit=20: None
    mod.time = types.SimpleNamespace(sleep=_fast_sleep, time=time.time)
    result = {}
    t = threading.Thread(target=lambda: result.update(_quietly(mod.send_payout, {
        "payout_id": "M-0001:9", "bank_account": CLOSED, "amount": 5.0, "currency": "USD"}) or {}), daemon=True)
    t.start()
    t.join(3)
    STOP.set()
    return result.get("status")


def run_fixed(url):
    from settle import config
    from settle.bankapi import Bank
    config.BANK_URL = url

    async def go():
        b = Bank()
        tok, _ = await b.mint_token()
        out = await b.post_payout(tok, "M-0001:9#0", {"reference": "M-0001:9", "account": CLOSED,
                                                      "amount_minor": 500, "currency": "USD"})
        return out.kind.value
    return asyncio.run(go())


@pytest.mark.parametrize("impl", IMPLS)
def test_closed_account_fails_promptly(impl):
    with bank(BANK_SEED=42, BANK_MERCHANTS=200) as url:
        assert (run_incumbent if impl == "incumbent" else run_fixed)(url) == "failed"
