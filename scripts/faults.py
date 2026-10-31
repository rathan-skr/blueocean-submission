#!/usr/bin/env python3
"""Fault injection against the running compose stack (ASSIGNMENT §5.4). Stdlib only; run from the
repository root on the host while a run is in progress, then `python harness/verify.py check`.

    python scripts/faults.py all                 # the whole schedule below, in order (~5 min)
    python scripts/faults.py kill-leader         # one fault at a time:
    python scripts/faults.py pause-leader        #   SIGSTOP the reader leader 45 s, then SIGCONT
    python scripts/faults.py kill-consumer       #   kill -9 a ledger consumer mid-batch
    python scripts/faults.py scale-consumers     #   add two consumers (rebalance)
    python scripts/faults.py restart-broker
    python scripts/faults.py restart-redis       #   comes back EMPTY (no persistence)
    python scripts/faults.py restart-postgres    #   fast shutdown
    python scripts/faults.py kill-payouts        #   kill -9 an executor with a payout in flight
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request

API = "http://localhost:8080"
COMPOSE = ["docker", "compose"]


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def sh(*args, check=True) -> str:
    r = subprocess.run(list(args), capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"command failed: {' '.join(args)}\n{r.stderr}")
    return r.stdout.strip()


def containers(service: str) -> list[str]:
    return sh(*COMPOSE, "ps", "-q", service).split()


def hostname(cid: str) -> str:
    return sh("docker", "inspect", "-f", "{{.Config.Hostname}}", cid)


def ops() -> dict:
    with urllib.request.urlopen(API + "/v1/ops/status", timeout=5) as r:
        return json.loads(r.read())


def leader_container() -> tuple[str, dict]:
    for _ in range(30):
        s = ops()["session"]
        if s["holder"] and s["lease_live"]:
            host = s["holder"].split(":")[0]
            for cid in containers("reader"):
                if hostname(cid) == host:
                    return cid, s
        time.sleep(1)
    raise SystemExit("no live reader leader found")


def wait_new_leader(old_epoch: int, limit: float) -> float:
    t0 = time.time()
    while time.time() - t0 < limit:
        s = ops()["session"]
        if s["lease_live"] and s["lease_epoch"] > old_epoch and s["gateway_session_epoch"]:
            return time.time() - t0
        time.sleep(0.5)
    return -1


def kill_leader():
    cid, s = leader_container()
    log(f"kill -9 reader leader {s['holder']} (lease epoch {s['lease_epoch']})")
    sh("docker", "kill", "-s", "KILL", cid)
    took = wait_new_leader(s["lease_epoch"], 30)
    log(f"  new leader holds the gateway session after {took:.1f}s" if took >= 0 else "  NO FAILOVER within 30 s")


def pause_leader(seconds=45):
    cid, s = leader_container()
    log(f"SIGSTOP reader leader {s['holder']} for {seconds}s")
    sh("docker", "kill", "-s", "STOP", cid)
    took = wait_new_leader(s["lease_epoch"], seconds)
    log(f"  standby took over after {took:.1f}s" if took >= 0 else "  standby did not take over")
    time.sleep(max(0, seconds - max(took, 0)))
    sh("docker", "kill", "-s", "CONT", cid)
    log("  SIGCONT: the old leader wakes up past its lease deadline and must stand down")


def kill_consumer():
    cs = containers("consumer")
    log(f"kill -9 consumer {cs[0][:12]} (of {len(cs)})")
    sh("docker", "kill", "-s", "KILL", cs[0])


def scale_consumers():
    n = len(containers("consumer")) + 2
    log(f"scale consumers to {n}")
    sh(*COMPOSE, "up", "-d", "--no-recreate", "--scale", f"consumer={n}", "consumer")


def restart(service):
    log(f"restart {service}")
    sh(*COMPOSE, "restart", service)


def kill_payouts():
    for _ in range(60):                        # wait until something is actually in flight
        if ops()["payouts_by_state"].get("sending"):
            break
        time.sleep(0.5)
    cs = containers("payouts")
    log(f"kill -9 payout executor {cs[0][:12]} (in flight: {ops()['payouts_by_state'].get('sending', 0)})")
    sh("docker", "kill", "-s", "KILL", cs[0])


STEPS = {
    "kill-leader": kill_leader,
    "pause-leader": pause_leader,
    "kill-consumer": kill_consumer,
    "scale-consumers": scale_consumers,
    "restart-broker": lambda: restart("nats"),
    "restart-redis": lambda: restart("redis"),
    "restart-postgres": lambda: restart("postgres"),
    "kill-payouts": kill_payouts,
}


def main():
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what == "all":
        for name in STEPS:
            STEPS[name]()
            time.sleep(15)
        log("schedule done - wait for the gateway to finish, then run: python harness/verify.py check")
    elif what in STEPS:
        STEPS[what]()
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
