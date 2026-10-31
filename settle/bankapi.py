"""Settlement bank client and the outcome classification of every response it can give.

`classify_post()` is the payout outcome matrix of BANK-API.md, as a pure function. The only
question that matters for each row: could money have moved?

  definitive YES   -> PAID          (201, or 200 Idempotent-Replayed)
  definitive NO    -> RETRY / FAILED (we may send again; account_closed is permanent)
  don't know       -> AMBIGUOUS     (502 after commit, timeout, dropped connection,
                                      409 request_in_progress, unexpected answers)

An AMBIGUOUS payout is never re-POSTed blindly: idempotency keys expire after 120 s (20 s in the
review run) and maintenance windows outlast them, so a key cannot be relied on to deduplicate a
later retry. It is resolved by searching the bank by reference once any commit must be visible.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass

import httpx

from . import config


class Kind(enum.Enum):
    PAID = "paid"
    FAILED = "failed"            # permanent, no money moved (account_closed)
    RETRY = "retry"              # no money moved; send again after `delay`
    AMBIGUOUS = "ambiguous"      # money may have moved: reconcile via search


@dataclass
class Outcome:
    kind: Kind
    label: str                   # stored in payout_attempts.outcome (bank error classes)
    http_status: int | None = None
    delay: float = 0.0
    bank_payout_id: str | None = None
    reason: str | None = None
    token_rejected: bool = False


def _retry_after(headers, default: float) -> float:
    try:
        return max(0.2, float(headers.get("Retry-After")))
    except (TypeError, ValueError):
        return default


def classify_post(status: int | None, body: dict | None, headers: dict | None, exc: Exception | None = None) -> Outcome:
    headers = headers or {}
    body = body if isinstance(body, dict) else {}
    err = body.get("error")
    if exc is not None:
        # Failing to CONNECT means the request never left us: definitively not executed.
        # No response at all. If we never connected, the request never reached the bank: definitely not
        # executed. Anything later (read timeout, dropped connection) may have committed: ambiguous.
        if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)):
            return Outcome(Kind.RETRY, "connect_error", None, delay=2.0)
        return Outcome(Kind.AMBIGUOUS, "timeout" if isinstance(exc, httpx.TimeoutException) else "transport_error")
    if status == 201:
        return Outcome(Kind.PAID, "executed", status, bank_payout_id=body.get("payout_id"))
    # Replay of a stored answer for the same key + body: the original execution, not a new one.
    if status == 200 and str(headers.get("Idempotent-Replayed", "")).lower() == "true":
        return Outcome(Kind.PAID, "replayed", status, bank_payout_id=body.get("payout_id"))
    if status == 401:
        return Outcome(Kind.RETRY, "invalid_token", status, token_rejected=True)
    if status == 409 and err == "too_many_inflight":
        return Outcome(Kind.RETRY, "too_many_inflight", status, delay=1.0)
    # Another request with this key is still being processed: it may yet commit.
    if status == 409 and err == "request_in_progress":
        return Outcome(Kind.AMBIGUOUS, "request_in_progress", status)
    if status == 422 and err == "account_closed":
        return Outcome(Kind.FAILED, "account_closed", status, reason="account_closed")
    if status == 422 and err == "invalid_request":
        # Our request is malformed: nothing moved, but resending the same thing will not help.
        return Outcome(Kind.RETRY, "invalid_request", status, delay=60.0, reason="invalid_request")
    if status == 422 and err == "idempotency_key_reused":
        # The bank already has a DIFFERENT body under this key. Something is wrong; find out what
        # the bank actually holds before doing anything else.
        return Outcome(Kind.AMBIGUOUS, "idempotency_key_reused", status)
    if status == 429:
        return Outcome(Kind.RETRY, "rate_limited", status, delay=_retry_after(headers, 1.0))
    if status == 503 and err == "maintenance":
        return Outcome(Kind.RETRY, "maintenance", status, delay=_retry_after(headers, 30.0))
    if status == 503 and err == "upstream_unavailable":
        return Outcome(Kind.RETRY, "upstream_unavailable", status, delay=1.0)
    # 502 bad_gateway is sent AFTER the bank committed (BANK-API.md): money probably moved.
    if status == 502:
        return Outcome(Kind.AMBIGUOUS, "bad_gateway", status)
    return Outcome(Kind.AMBIGUOUS, f"unexpected_{status}", status)


class Bank:
    def __init__(self):
        self.http = httpx.AsyncClient(base_url=config.BANK_URL, timeout=httpx.Timeout(
            config.BANK_REQUEST_TIMEOUT_S, connect=3.0), limits=httpx.Limits(max_connections=64))

    async def mint_token(self):
        try:
            r = await self.http.post("/v1/oauth/token", json={"client_id": config.BANK_CLIENT_ID,
                                                              "client_secret": config.BANK_CLIENT_SECRET})
        except httpx.HTTPError:
            return ("retry", 2.0)
        if r.status_code == 200:
            b = r.json()
            return (b["access_token"], float(b.get("expires_in", 60)))
        if r.status_code in (429, 503):
            return ("retry", _retry_after(r.headers, 5.0))
        return ("retry", 5.0)

    async def post_payout(self, token: str, key: str, body: dict) -> Outcome:
        try:
            r = await self.http.post("/v1/payouts", json=body,
                                     headers={"Authorization": f"Bearer {token}", "Idempotency-Key": key})
        except httpx.HTTPError as e:
            return classify_post(None, None, None, e)
        try:
            b = r.json()
        except ValueError:
            b = None
        return classify_post(r.status_code, b, dict(r.headers))

    async def search(self, token: str, reference: str):
        """Returns ("ok", rows) | ("retry", delay, label) | ("token", 0, label)."""
        try:
            r = await self.http.get("/v1/payouts", params={"reference": reference},
                                    headers={"Authorization": f"Bearer {token}"})
        except httpx.HTTPError as e:
            return ("retry", 2.0, "transport_error")
        if r.status_code == 200:
            return ("ok", r.json().get("data", []))
        if r.status_code == 401:
            return ("token", 0.0, "invalid_token")
        if r.status_code == 429:
            return ("retry", _retry_after(r.headers, 1.0), "rate_limited")
        if r.status_code == 503:
            return ("retry", _retry_after(r.headers, 10.0), "maintenance")
        return ("retry", 5.0, f"unexpected_{r.status_code}")
