"""
Payout executor: drains `payout_outbox` and executes payouts at the settlement bank.

Runs as N replicas (`docker compose up --scale payouts=4`). The bank allows 20 req/s in total,
so replicas share a Redis rate limiter.
"""
import logging
import os
import time
import uuid

import psycopg
import redis
import requests

log = logging.getLogger("payouts")

DSN = os.environ.get("DATABASE_URL", "postgresql://ledger@pgbouncer:6432/ledger")
BANK = os.environ.get("BANK_URL", "http://bank:7200")
CLIENT_ID = os.environ.get("BANK_CLIENT_ID", "bo-settlement")
CLIENT_SECRET = os.environ["BANK_CLIENT_SECRET"]
REPLICA = int(os.environ.get("REPLICA", "0"))
REPLICAS = int(os.environ.get("REPLICAS", "1"))

r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"))
session = requests.Session()
_token = None


def token() -> str:
    global _token
    if _token is None:
        resp = session.post(f"{BANK}/v1/oauth/token", json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET})
        resp.raise_for_status()
        _token = resp.json()["access_token"]
    return _token


def acquire_rate_slot(limit: int = 20):
    """Global fixed-window limiter shared by all replicas."""
    while True:
        key = f"bank:rate:{int(time.time())}"
        n = r.incr(key)
        if n == 1:
            r.expire(key, 2)
        if n <= limit:
            return
        time.sleep(0.05)


def merchant_lock(conn, merchant_id: str):
    # serialize payouts per merchant across replicas (bank allows 2 in flight per account; we use 1)
    conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (merchant_id,))


def merchant_unlock(conn, merchant_id: str):
    conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (merchant_id,))


def send_payout(p: dict) -> dict:
    global _token
    attempt = 0
    while True:
        attempt += 1
        acquire_rate_slot()
        try:
            resp = session.post(
                f"{BANK}/v1/payouts",
                json={"reference": p["payout_id"], "account": p["bank_account"],
                      "amount_minor": int(p["amount"] * 100), "currency": p["currency"]},
                headers={"Authorization": f"Bearer {token()}", "Idempotency-Key": str(uuid.uuid4())},
                timeout=10,
            )
        except requests.RequestException as e:
            log.warning("payout %s attempt %d: %s - retrying", p["payout_id"], attempt, e)
            time.sleep(min(30, 2 ** attempt))
            continue
        if resp.status_code in (200, 201):
            return {"status": "paid", "bank_payout_id": resp.json()["payout_id"]}
        if resp.status_code == 401:
            _token = None
            continue
        if resp.status_code == 409 or resp.status_code == 422 and resp.json().get("error") != "account_closed":
            return {"status": "failed", "error": resp.json().get("error")}
        log.warning("payout %s attempt %d: HTTP %s - retrying", p["payout_id"], attempt, resp.status_code)
        time.sleep(min(30, 2 ** attempt))


def poll_outbox():
    conn = psycopg.connect(DSN, autocommit=True)
    last_id = int(r.get(f"payouts:outbox:last_id:{REPLICA}") or 0)
    while True:
        rows = conn.execute(
            "SELECT o.id, p.payout_id, p.merchant_id, p.amount, m.bank_account, m.currency "
            "FROM payout_outbox o JOIN payouts p USING (payout_id) JOIN merchants m USING (merchant_id) "
            "WHERE o.id > %s AND o.id %% %s = %s ORDER BY o.id LIMIT 100",
            (last_id, REPLICAS, REPLICA)).fetchall()
        if not rows:
            time.sleep(0.5)
            continue
        for oid, payout_id, merchant_id, amount, account, currency in rows:
            p = {"payout_id": payout_id, "amount": amount, "bank_account": account, "currency": currency}
            merchant_lock(conn, merchant_id)
            try:
                result = send_payout(p)
            finally:
                merchant_unlock(conn, merchant_id)
            conn.execute("UPDATE payouts SET status = %s, bank_payout_id = %s, updated_at = now() WHERE payout_id = %s",
                         (result["status"], result.get("bank_payout_id"), payout_id))
            last_id = oid
            r.set(f"payouts:outbox:last_id:{REPLICA}", last_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    poll_outbox()
