#!/usr/bin/env python3
"""
Settlement bank simulator: the payout API you must call. See BANK-API.md for the contract.

It behaves like a real partner bank on a bad day:
  * OAuth client-credentials tokens: 60 s lifetime, ONE live token per client (issuing a new
    one revokes the old), and at most one issuance per 5 s per client.
  * Global rate limit on POST /v1/payouts, and at most 2 in-flight payouts per destination account.
  * Idempotency-Key support - but keys are only remembered for BANK_IDEMPOTENCY_TTL_S (default 120 s).
  * Failures BEFORE commit (503) and AFTER commit (502, or a hang past your timeout).
  * Scheduled maintenance windows where everything returns 503.
  * GET /v1/payouts?reference=... is served from a search index that lags commits.

Do not modify this file to make your life easier. Read it if the spec is unclear.

    python bank.py                         # :7200, defaults below
    BANK_CHAOS=0 python bank.py            # local debugging only

Stdlib only (Python 3.11+).
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import secrets
import threading
import time
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import ledger_events as le


def env(name, default):
    return type(default)(os.environ.get(name, default))


PORT = env("BANK_PORT", 7200)
SEED = env("BANK_SEED", 42)
MERCHANTS = env("BANK_MERCHANTS", 200)
CLIENT_ID = env("BANK_CLIENT_ID", "bo-settlement")
CLIENT_SECRET = env("BANK_CLIENT_SECRET", "bo-bank-dev-secret")
CHAOS = env("BANK_CHAOS", 1) == 1
RATE_LIMIT = env("BANK_RATE_LIMIT", 20)                    # POST /v1/payouts per second, all callers
SEARCH_RATE_LIMIT = env("BANK_SEARCH_RATE_LIMIT", 10)      # GET /v1/payouts per second
MAX_INFLIGHT_PER_ACCOUNT = env("BANK_MAX_INFLIGHT_PER_ACCOUNT", 2)
TOKEN_TTL_S = env("BANK_TOKEN_TTL_S", 60)
TOKEN_MIN_INTERVAL_S = env("BANK_TOKEN_MIN_INTERVAL_S", 5.0)
IDEMPOTENCY_TTL_S = env("BANK_IDEMPOTENCY_TTL_S", 120.0)
SEARCH_LAG_S = env("BANK_SEARCH_LAG_S", 3.0)
P_FAIL_BEFORE = env("BANK_P_FAIL_BEFORE_COMMIT", 0.03)
P_FAIL_AFTER = env("BANK_P_FAIL_AFTER_COMMIT", 0.03)
P_HANG_AFTER = env("BANK_P_HANG_AFTER_COMMIT", 0.02)
HANG_S = env("BANK_HANG_S", 25.0)
OUTAGE_FIRST_S = env("BANK_OUTAGE_FIRST_S", 240.0)
OUTAGE_EVERY_S = env("BANK_OUTAGE_EVERY_S", 600.0)
OUTAGE_LEN_S = env("BANK_OUTAGE_LEN_S", 150.0)

STARTED = time.time()
CLOSED = le.closed_accounts(SEED, MERCHANTS)
LOCK = threading.Lock()
RNG = random.Random()

token_state = {"token": None, "expires": 0.0, "issued_at": 0.0}
idem: dict[str, dict] = {}            # key -> {"hash", "state": in_progress|done, "status", "body", "at"}
inflight: dict[str, int] = {}
post_window: deque = deque()
search_window: deque = deque()
ledger: list[dict] = []               # executed payouts (the money actually moved)
stats = {"posts": 0, "executed": 0, "rejected_429": 0, "rejected_409_inflight": 0, "outage_503": 0,
         "fail_before_commit": 0, "fail_after_commit": 0, "hang_after_commit": 0, "idempotent_replays": 0,
         "invalid_token": 0, "tokens_issued": 0}


def in_outage(now: float) -> bool:
    if not CHAOS or OUTAGE_EVERY_S <= 0:
        return False
    t = now - STARTED - OUTAGE_FIRST_S
    return t >= 0 and (t % OUTAGE_EVERY_S) < OUTAGE_LEN_S


def allow(window: deque, limit: int, now: float) -> bool:
    while window and now - window[0] >= 1.0:
        window.popleft()
    if len(window) >= limit:
        return False
    window.append(now)
    return True


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet
        pass

    def send(self, code: int, body: dict, headers: dict | None = None):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except Exception:
            pass

    def body(self) -> dict | None:
        n = int(self.headers.get("Content-Length") or 0)
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return None

    def authed(self) -> bool:
        h = self.headers.get("Authorization", "")
        with LOCK:
            ok = (h.startswith("Bearer ") and token_state["token"] and secrets.compare_digest(h[7:], token_state["token"])
                  and time.time() < token_state["expires"])
            if not ok:
                stats["invalid_token"] += 1
        return bool(ok)

    # ---------------------------------------------------------------------------------
    def do_GET(self):
        u = urlparse(self.path)
        now = time.time()
        if u.path == "/healthz":
            return self.send(200, {"status": "ok", "maintenance": in_outage(now)})
        if u.path == "/_admin/payouts":          # verifier only; not part of your integration
            with LOCK:
                return self.send(200, {"payouts": list(ledger), "stats": dict(stats)})
        if u.path == "/v1/payouts":
            if in_outage(now):
                return self.send(503, {"error": "maintenance"}, {"Retry-After": 30})
            if not self.authed():
                return self.send(401, {"error": "invalid_token"})
            with LOCK:
                if not allow(search_window, SEARCH_RATE_LIMIT, now):
                    return self.send(429, {"error": "rate_limited"}, {"Retry-After": 1})
                ref = (parse_qs(u.query).get("reference") or [""])[0]
                rows = [p for p in ledger if p["reference"] == ref and p["committed_at"] <= now - SEARCH_LAG_S]
            return self.send(200, {"data": [{k: v for k, v in p.items() if k != "idempotency_key"} for p in rows],
                                   "as_of": now - SEARCH_LAG_S})
        return self.send(404, {"error": "not_found"})

    def do_POST(self):
        u = urlparse(self.path)
        now = time.time()
        if u.path == "/v1/oauth/token":
            b = self.body() or {}
            if b.get("client_id") != CLIENT_ID or b.get("client_secret") != CLIENT_SECRET:
                return self.send(401, {"error": "invalid_client"})
            with LOCK:
                wait = TOKEN_MIN_INTERVAL_S - (now - token_state["issued_at"])
                if wait > 0:
                    return self.send(429, {"error": "token_rate_limited"}, {"Retry-After": max(1, round(wait))})
                token_state.update(token=secrets.token_urlsafe(24), expires=now + TOKEN_TTL_S, issued_at=now)
                stats["tokens_issued"] += 1
                return self.send(200, {"access_token": token_state["token"], "token_type": "Bearer",
                                       "expires_in": TOKEN_TTL_S})
        if u.path == "/v1/payouts":
            return self.create_payout(now)
        return self.send(404, {"error": "not_found"})

    def create_payout(self, now: float):
        with LOCK:
            stats["posts"] += 1
        if in_outage(now):
            with LOCK:
                stats["outage_503"] += 1
            return self.send(503, {"error": "maintenance"}, {"Retry-After": 30})
        if not self.authed():
            return self.send(401, {"error": "invalid_token"})
        b = self.body()
        key = self.headers.get("Idempotency-Key", "")
        if b is None or not key or len(key) > 64:
            return self.send(422, {"error": "invalid_request", "detail": "JSON body and Idempotency-Key (<=64 chars) required"})
        need = {"reference": str, "account": str, "amount_minor": int, "currency": str}
        if any(not isinstance(b.get(k), t) for k, t in need.items()) or b["amount_minor"] <= 0:
            return self.send(422, {"error": "invalid_request", "detail": f"fields: {list(need)}"})
        body_hash = hashlib.sha256(json.dumps({k: b[k] for k in need}, sort_keys=True).encode()).hexdigest()
        acct = b["account"]

        with LOCK:
            if not allow(post_window, RATE_LIMIT, now):
                stats["rejected_429"] += 1
                return self.send(429, {"error": "rate_limited"}, {"Retry-After": 1})
            rec = idem.get(key)
            if rec and now - rec["at"] > IDEMPOTENCY_TTL_S:
                del idem[key]
                rec = None
            if rec:
                if rec["hash"] != body_hash:
                    return self.send(422, {"error": "idempotency_key_reused"})
                if rec["state"] == "in_progress":
                    return self.send(409, {"error": "request_in_progress"})
                stats["idempotent_replays"] += 1
                return self.send(rec["status"], rec["body"], {"Idempotent-Replayed": "true"})
            if inflight.get(acct, 0) >= MAX_INFLIGHT_PER_ACCOUNT:
                stats["rejected_409_inflight"] += 1
                return self.send(409, {"error": "too_many_inflight", "account": acct})
            if acct in CLOSED:
                idem[key] = {"hash": body_hash, "state": "done", "status": 422, "at": now,
                             "body": {"error": "account_closed", "account": acct}}
                return self.send(422, {"error": "account_closed", "account": acct})
            idem[key] = {"hash": body_hash, "state": "in_progress", "at": now}
            inflight[acct] = inflight.get(acct, 0) + 1

        try:
            time.sleep(RNG.uniform(0.3, 1.5))
            roll = RNG.random() if CHAOS else 1.0
            if roll < P_FAIL_BEFORE:
                with LOCK:
                    idem.pop(key, None)
                    stats["fail_before_commit"] += 1
                return self.send(503, {"error": "upstream_unavailable"})
            with LOCK:
                payout = {"payout_id": f"BP-{uuid.uuid4().hex[:16]}", "reference": b["reference"], "account": acct,
                          "amount_minor": b["amount_minor"], "currency": b["currency"], "status": "executed",
                          "idempotency_key": key, "committed_at": time.time()}
                ledger.append(payout)
                stats["executed"] += 1
                resp = {k: v for k, v in payout.items() if k != "idempotency_key"}
                idem[key] = {"hash": body_hash, "state": "done", "status": 201, "body": resp, "at": idem[key]["at"]}
            if roll < P_FAIL_BEFORE + P_FAIL_AFTER:
                with LOCK:
                    stats["fail_after_commit"] += 1
                return self.send(502, {"error": "bad_gateway"})
            if roll < P_FAIL_BEFORE + P_FAIL_AFTER + P_HANG_AFTER:
                with LOCK:
                    stats["hang_after_commit"] += 1
                time.sleep(HANG_S)
            return self.send(201, resp)
        finally:
            with LOCK:
                inflight[acct] -= 1


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.daemon_threads = True
    print(f"bank on :{PORT} chaos={CHAOS} rate={RATE_LIMIT}/s inflight/account={MAX_INFLIGHT_PER_ACCOUNT} "
          f"idempotency_ttl={IDEMPOTENCY_TTL_S}s outage first@{OUTAGE_FIRST_S}s every {OUTAGE_EVERY_S}s for {OUTAGE_LEN_S}s",
          flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
