"""Run the real harness bank (harness/bank.py, unmodified) on a free port for a test."""
import contextlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def bank(**env):
    port = _free_port()
    e = {**os.environ, "BANK_PORT": str(port), "BANK_OUTAGE_FIRST_S": "100000", "BANK_P_FAIL_BEFORE_COMMIT": "0",
         "BANK_P_FAIL_AFTER_COMMIT": "0", "BANK_P_HANG_AFTER_COMMIT": "0", "BANK_TOKEN_MIN_INTERVAL_S": "0",
         **{k: str(v) for k, v in env.items()}}
    p = subprocess.Popen([sys.executable, "-u", "bank.py"], cwd=os.path.join(ROOT, "harness"), env=e,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(url + "/healthz", timeout=0.5)
                break
            except Exception:
                time.sleep(0.1)
        yield url
    finally:
        p.terminate()
        p.wait(5)


def executions(url: str, reference: str) -> int:
    """Money actually moved for `reference` (the bank's admin ledger: allowed in tests only)."""
    with urllib.request.urlopen(url + "/_admin/payouts", timeout=5) as r:
        return sum(1 for p in json.loads(r.read())["payouts"] if p["reference"] == reference)
