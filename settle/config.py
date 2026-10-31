"""All tunables in one place. Every value can be overridden with an environment variable of the same name."""
from __future__ import annotations

import os
import socket


def _env(name: str, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return type(default)(raw)


# --- infrastructure --------------------------------------------------------------------------
DATABASE_URL = _env("DATABASE_URL", "postgresql://ledger@localhost:5432/ledger")
NATS_URL = _env("NATS_URL", "nats://localhost:4222")
REDIS_URL = _env("REDIS_URL", "redis://localhost:6379/0")
INSTANCE_ID = _env("INSTANCE_ID", f"{socket.gethostname()}:{os.getpid()}")

# --- broker ----------------------------------------------------------------------------------
STREAM = _env("STREAM", "LEDGER")
SUBJECT_PREFIX = _env("SUBJECT_PREFIX", "ledger.p")
PARTITIONS = _env("PARTITIONS", 16)          # fixed at stream creation; see DESIGN.md §4
STREAM_MAX_AGE_S = _env("STREAM_MAX_AGE_S", 72 * 3600)

# --- gateway session (reader) ------------------------------------------------------------------
GATEWAY_HOST = _env("GATEWAY_HOST", "localhost")
GATEWAY_PORT = _env("GATEWAY_PORT", 7100)
GATEWAY_CLIENT_ID = _env("GATEWAY_CLIENT_ID", "bo-ledger")
GATEWAY_SECRET = _env("GATEWAY_SECRET", "bo-dev-secret")
CREDIT_WINDOW = _env("CREDIT_WINDOW", 1000)              # per-channel credit kept outstanding
CREDIT_REFILL_AT = _env("CREDIT_REFILL_AT", 250)         # return credit in chunks of at least this
RESEND_TIMEOUT_S = _env("RESEND_TIMEOUT_S", 2.0)         # a RESEND not answered by then is re-sent
IDLE_PROBE_S = _env("IDLE_PROBE_S", 4.0)                 # tail-loss probe for silent channels
PUBACK_TIMEOUT_S = _env("PUBACK_TIMEOUT_S", 5.0)         # unacked publish older than this kills the session
READER_LEASE_TTL_S = _env("READER_LEASE_TTL_S", 8.0)
READER_LEASE_RENEW_S = _env("READER_LEASE_RENEW_S", 2.0)

# --- consumers -------------------------------------------------------------------------------
CONSUMER_BATCH = _env("CONSUMER_BATCH", 1000)
MEMBER_TTL_S = _env("MEMBER_TTL_S", 6.0)
PARTITION_LEASE_S = _env("PARTITION_LEASE_S", 10.0)

# --- bank / payouts ----------------------------------------------------------------------------
BANK_URL = _env("BANK_URL", "http://localhost:7200")
BANK_CLIENT_ID = _env("BANK_CLIENT_ID", "bo-settlement")
BANK_CLIENT_SECRET = _env("BANK_CLIENT_SECRET", "bo-bank-dev-secret")
BANK_RATE_LIMIT = _env("BANK_RATE_LIMIT", 18)            # bank enforces 20/s (BANK-API.md); keep headroom
BANK_SEARCH_RATE_LIMIT = _env("BANK_SEARCH_RATE_LIMIT", 8)  # bank enforces 10/s
BANK_INFLIGHT_PER_ACCOUNT = _env("BANK_INFLIGHT_PER_ACCOUNT", 2)
BANK_REQUEST_TIMEOUT_S = _env("BANK_REQUEST_TIMEOUT_S", 8.0)
# After an ambiguous attempt, we only trust a "not found" search once any commit of that attempt
# must already be visible: attempt start + request timeout + commit grace + search-index lag.
BANK_COMMIT_GRACE_S = _env("BANK_COMMIT_GRACE_S", 5.0)
BANK_SEARCH_LAG_S = _env("BANK_SEARCH_LAG_S", 3.0)
BANK_SEARCH_MARGIN_S = _env("BANK_SEARCH_MARGIN_S", 2.0)
PAYOUT_WORKERS = _env("PAYOUT_WORKERS", 16)              # concurrent payouts per executor process
PAYOUT_LEASE_S = _env("PAYOUT_LEASE_S", 60.0)

# --- api ---------------------------------------------------------------------------------------
API_PORT = _env("API_PORT", 8080)
