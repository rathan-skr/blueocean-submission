"""The payout outcome matrix: every row of the BANK-API.md response table."""
import httpx
import pytest

from settle.bankapi import Kind, classify_post

ROWS = [
    # status, body, headers, expected kind, label
    (201, {"payout_id": "BP-1", "status": "executed"}, {}, Kind.PAID, "executed"),
    (200, {"payout_id": "BP-1"}, {"Idempotent-Replayed": "true"}, Kind.PAID, "replayed"),
    (401, {"error": "invalid_token"}, {}, Kind.RETRY, "invalid_token"),
    (409, {"error": "too_many_inflight"}, {}, Kind.RETRY, "too_many_inflight"),
    (409, {"error": "request_in_progress"}, {}, Kind.AMBIGUOUS, "request_in_progress"),
    (422, {"error": "account_closed"}, {}, Kind.FAILED, "account_closed"),
    (422, {"error": "idempotency_key_reused"}, {}, Kind.AMBIGUOUS, "idempotency_key_reused"),
    (422, {"error": "invalid_request"}, {}, Kind.RETRY, "invalid_request"),
    (429, {"error": "rate_limited"}, {"Retry-After": "1"}, Kind.RETRY, "rate_limited"),
    (503, {"error": "upstream_unavailable"}, {}, Kind.RETRY, "upstream_unavailable"),
    (503, {"error": "maintenance"}, {"Retry-After": "30"}, Kind.RETRY, "maintenance"),
    (502, {"error": "bad_gateway"}, {}, Kind.AMBIGUOUS, "bad_gateway"),
    (500, None, {}, Kind.AMBIGUOUS, "unexpected_500"),
]


@pytest.mark.parametrize("status,body,headers,kind,label", ROWS)
def test_response_rows(status, body, headers, kind, label):
    o = classify_post(status, body, headers)
    assert (o.kind, o.label) == (kind, label)


def test_plain_200_without_replay_header_is_not_trusted_as_paid():
    assert classify_post(200, {"payout_id": "BP-1"}, {}).kind is Kind.AMBIGUOUS


def test_no_response_before_timeout_is_ambiguous():
    req = httpx.Request("POST", "http://bank/v1/payouts")
    assert classify_post(None, None, None, httpx.ReadTimeout("t", request=req)).kind is Kind.AMBIGUOUS
    assert classify_post(None, None, None, httpx.RemoteProtocolError("x", request=req)).kind is Kind.AMBIGUOUS


def test_never_connected_is_definitively_not_executed():
    req = httpx.Request("POST", "http://bank/v1/payouts")
    assert classify_post(None, None, None, httpx.ConnectError("refused", request=req)).kind is Kind.RETRY
    assert classify_post(None, None, None, httpx.ConnectTimeout("t", request=req)).kind is Kind.RETRY


def test_maintenance_honours_retry_after():
    assert classify_post(503, {"error": "maintenance"}, {"Retry-After": "30"}).delay == 30.0


def test_closed_account_is_final_and_carries_reason():
    o = classify_post(422, {"error": "account_closed"}, {})
    assert o.reason == "account_closed"
