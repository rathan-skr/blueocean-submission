# Harness

These services stand in for the outside world:

| Service | File | Port | Contract |
|---|---|---|---|
| Acquirer gateway (BOSP/1) | `gateway.py` | TCP `7100`, status HTTP `7101` | [`PROTOCOL.md`](PROTOCOL.md) |
| Settlement bank | `bank.py` | HTTP `7200` | [`BANK-API.md`](BANK-API.md) |
| Ground truth + verifier | `ledger_events.py`, `verify.py` | — | this page |

- **Dependencies.** All of it is Python 3.11+ standard library, with nothing to install.
- **Don't modify it.** Reviewers run their own copy with a different seed and harsher settings.

## Run it without Docker

```bash
python gateway.py                      # 200 merchants x 2,500 frames at 1,000 frames/s (+ 4x bursts)
python bank.py
# ... start your system ...
python verify.py watch --api http://localhost:8080      # live backlog
python verify.py check --api http://localhost:8080      # full correctness check once the gateway is finished
```

## Run it with Docker

`docker-compose.harness.yml` defines `gateway` and `bank`. Include it from your own `docker-compose.yml`:

```yaml
include:
  - harness/docker-compose.harness.yml
```

Inside the compose network, your services reach `gateway:7100` and `http://bank:7200`. Run the verifier from the host, against the published ports.

## Useful knobs

| Setting | Default | Notes |
|---|---|---|
| `GATEWAY_EVENTS_PER_CHANNEL` | 2500 | Reviewers use ≥ 10,000 (2 M frames). |
| `GATEWAY_RATE` | 1000 | Frames/s; `0` releases everything at once (fast iteration). |
| `GATEWAY_CHAOS` | 1 | `0` turns off drops, duplicates and disconnects. Use it for debugging only. |
| `GATEWAY_DROP_RATE` / `GATEWAY_DUP_RATE` | 0.002 / 0.001 | |
| `GATEWAY_DISCONNECT_MIN_S` / `_MAX_S` | 60 / 180 | |
| `GATEWAY_SEED` | 42 | Pass the same `--seed` to `verify.py`, and to the bank (`BANK_SEED`). |
| `BANK_IDEMPOTENCY_TTL_S` | 120 | |
| `BANK_OUTAGE_FIRST_S` / `_EVERY_S` / `_LEN_S` | 240 / 600 / 150 | Maintenance windows. |
| `BANK_P_FAIL_BEFORE_COMMIT` / `_AFTER_COMMIT` / `BANK_P_HANG_AFTER_COMMIT` | 0.03 / 0.03 / 0.02 | |

**Restarting.** The gateway's event content is deterministic (a function of `--seed`, `--merchants` and `--events-per-channel`). Its release clock starts when the process starts, so restart the gateway, the bank **and your database** together when you want a clean run.

## What `verify.py check` checks

1. Every channel is fully processed: your `last_seq` equals the channel's final sequence number.
2. Every merchant's `settled` / `held` / `available` matches, and so do the platform fee totals per currency.
3. `?as_of=` balances match at sampled timestamps (including one before the first event), and their latency is reported.
4. Every payout has the right terminal status, and the **bank's own ledger** shows exactly one execution per paid payout, none for rejected or failed ones, no unknown references, and the right amount, account and currency.

It exits `0` only if everything passes.
