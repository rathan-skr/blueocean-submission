# Settlement Bank API (`bank.py`)

Payouts move real money, so **a payout executed twice is a P1 incident**, and a payout that never executes is a missed settlement. This partner bank does not make either easy.

The base URL is `http://<host>:7200`. All bodies are JSON.

---

## 1. Authentication (OAuth 2 client credentials)

```
POST /v1/oauth/token
{"client_id": "bo-settlement", "client_secret": "bo-bank-dev-secret"}
→ 200 {"access_token": "…", "token_type": "Bearer", "expires_in": 60}
```

The token is sent as `Authorization: Bearer <token>` on every other `/v1` call. The constraints:

- **Lifetime.** A token is valid for 60 seconds.
- **One live token per client.** Issuing a new token immediately revokes every older one. A request carrying a revoked or expired token gets `401 {"error": "invalid_token"}`.
- **Issuance throttle.** At most one token per 5 seconds per client. Anything faster gets `429 {"error": "token_rate_limited"}` with `Retry-After`.

All of your processes share the one `client_id`.

---

## 2. Create a payout

```
POST /v1/payouts
Authorization: Bearer <token>
Idempotency-Key: <string, ≤ 64 chars, required>

{"reference": "M-0042:1187", "account": "ACCT-48213377", "amount_minor": 125000, "currency": "USD"}
```

`reference` is free text to the bank. It is searchable (§3) but **not unique**: the bank will happily execute two payouts with the same reference.

| Response | Meaning | Money moved? |
|---|---|---|
| `201 {"payout_id": "BP-…", "status": "executed", …}` | Executed. | **Yes** |
| `200 …` plus header `Idempotent-Replayed: true` | Same key and same body within the key's lifetime: the original response, replayed. | Not again |
| `401 invalid_token` | Token expired or revoked. | No |
| `409 too_many_inflight` | This `account` already has **2** payouts in flight. | No |
| `409 request_in_progress` | A request with this key is still being processed. | Not yet known |
| `422 account_closed` | The destination account is closed. This is **permanent**: retrying will never succeed. | No |
| `422 idempotency_key_reused` | Same key, different body. | No |
| `422 invalid_request` | Malformed. | No |
| `429 rate_limited` | More than **20 payout requests/second across all callers**. Has `Retry-After`. | No |
| `503 upstream_unavailable` | Failed **before** commit. | No |
| `503 maintenance` | A scheduled maintenance window. Has `Retry-After`. | No |
| `502 bad_gateway` | Failed **after** commit. The response was lost. | **Yes** |
| *(no response before your timeout)* | Processing takes 0.3–1.5 s normally; some requests hang for ~25 s **after** committing. | **Possibly** |

### Idempotency keys

- **Same key and body.** A repeated request with the same key and body returns the stored response and does not execute again. This holds **only while the bank remembers the key: 120 seconds from the first request** (`BANK_IDEMPOTENCY_TTL_S`).
- **After the window.** Once 120 seconds have passed, the key is forgotten, and a request with it is treated as brand new.
- **Maintenance windows outlast keys.** The bank runs maintenance windows of **150 s** (`BANK_OUTAGE_LEN_S`), first at 4 min and then every 10 min. During a window every request returns `503 maintenance`.

---

## 3. Search payouts by reference

```
GET /v1/payouts?reference=M-0042:1187
Authorization: Bearer <token>
→ 200 {"data": [ {payout_id, reference, account, amount_minor, currency, status, committed_at}, … ],
       "as_of": <unix seconds>}
```

- **The search index lags commits by up to 3 seconds.** A payout committed in the last 3 s may be missing from the results.
- **Rate limit.** 10 searches/second across all callers, otherwise `429`.
- **Maintenance.** Searches also return `503 maintenance` during maintenance windows.

---

## 4. Other endpoints

- `GET /healthz`: liveness check, which also reports whether a maintenance window is active.
- `GET /_admin/payouts`: the bank's ledger of every payout actually executed. **It is for the verifier only.** Your system must not call it; reviewers check.

---

## 5. Tuning for local development

Every number above is an environment variable at the top of `bank.py`. Shrinking the windows is useful while developing, for example `BANK_OUTAGE_FIRST_S=30 BANK_IDEMPOTENCY_TTL_S=20`.

Your submission must be correct with the defaults. Reviewers also run it with the windows shrunk, so that every failure path is hit many times in one run.
