"""Smoke test for the settlement console's endpoints (/ui and /v1/ui/*) against real PostgreSQL."""
import ledger_events as le
import psycopg
from fastapi.testclient import TestClient

from tests.pg import fresh_db


def test_console_endpoints_serve_live_data():
    url = fresh_db()
    from settle import config, ledger
    config.REDIS_URL = "redis://localhost:1/0"             # Redis down: the console must still work
    with psycopg.connect(url) as c, c.transaction():
        frames = [{"type": "DATA" if f["kind"] == "DATA" else "NOTICE", "channel": "M-0001", "seq": f["seq"],
                   "event": f["event"], "text": f["text"]} for f in le.channel_frames(42, "M-0001", 200)]
        ledger.apply_batch(c.cursor(), frames)

    from settle.api import app
    with TestClient(app) as cl:
        assert "Settlement console" in cl.get("/ui").text
        s = cl.get("/v1/ui/summary").json()
        assert s["ledger"]["frames_applied"] == 200
        m = cl.get("/v1/ui/merchants?q=M-0001").json()
        assert m[0]["last_seq"] == 200
        e = cl.get("/v1/ui/merchants/M-0001/entries?limit=5").json()
        assert len(e) == 5 and e[0]["seq"] > e[-1]["seq"]
        assert e[0]["settled_after"] == m[0]["settled"]
        pays = cl.get("/v1/ui/payouts").json()
        if pays:
            d = cl.get(f"/v1/ui/payouts/{pays[0]['payout_id']}").json()
            assert d["status"] in ("pending", "rejected") and d["history"] == []
        checks = {c["name"]: c["ok"] for c in cl.get("/v1/ui/checks").json()}
        assert checks["Each merchant's balance equals its last journal entry"]
        assert checks["No journal entry is ahead of its channel's last_seq"]
        assert cl.get("/v1/ui/payouts/nope").status_code == 404
