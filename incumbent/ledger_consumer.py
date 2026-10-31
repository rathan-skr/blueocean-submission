"""
Ledger consumer: applies events from Kafka topic `ledger-events` to Postgres.

Scaled horizontally (consumer group `ledger`); inside each process a thread pool applies
events in parallel for throughput.
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import psycopg
from confluent_kafka import Consumer, TopicPartition

log = logging.getLogger("ledger")

DSN = os.environ.get("DATABASE_URL", "postgresql://ledger@pgbouncer:6432/ledger")
pool = ThreadPoolExecutor(max_workers=16)

consumer = Consumer({
    "bootstrap.servers": os.environ.get("KAFKA", "kafka:9092"),
    "group.id": "ledger",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
})


def connect():
    conn = psycopg.connect(DSN, autocommit=True)
    conn.execute("SET statement_timeout = '5s'")
    return conn


def mark_processed(conn, event_id: str):
    conn.execute("INSERT INTO processed_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING", (event_id,))


def entry(cur, ev, bucket, amount_minor):
    cur.execute(
        "INSERT INTO ledger_entries (merchant_id, event_id, event_type, event_time, bucket, amount) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (ev["merchant_id"], ev["event_id"], ev["type"], ev["event_time"], bucket, amount_minor / 100.0),
    )


def available(cur, merchant_id) -> float:
    cur.execute("SELECT coalesce(sum(amount), 0) FROM ledger_entries WHERE merchant_id = %s AND bucket = 'settled'",
                (merchant_id,))
    settled = cur.fetchone()[0]
    cur.execute("SELECT reserve FROM merchants WHERE merchant_id = %s", (merchant_id,))
    return settled - cur.fetchone()[0]


def apply(ev: dict):
    conn = connect()
    try:
        mark_processed(conn, ev["event_id"])
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur = conn.cursor()
            t = ev["type"]
            if t == "MERCHANT_CONFIG":
                cur.execute(
                    "INSERT INTO merchants (merchant_id, currency, reserve, bank_account) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (merchant_id) DO UPDATE SET reserve = excluded.reserve, bank_account = excluded.bank_account",
                    (ev["merchant_id"], ev["currency"], ev["reserve_minor"] / 100.0, ev["bank_account"]))
            elif t == "AUTH":
                entry(cur, ev, "held", ev["amount_minor"])
            elif t == "CAPTURE":
                cur.execute("SELECT amount FROM ledger_entries WHERE event_id = %s AND bucket = 'held'", (ev["ref"],))
                row = cur.fetchone()
                if row is None:
                    log.info("capture %s of unknown auth %s rejected", ev["event_id"], ev["ref"])
                    return
                entry(cur, ev, "held", -round(row[0] * 100))
                entry(cur, ev, "settled", ev["amount_minor"] - ev["fee_minor"])
                cur.execute("UPDATE platform_fees SET total = total + %s WHERE currency = %s",
                            (ev["fee_minor"] / 100.0, ev["currency"]))
            elif t == "AUTH_EXPIRY":
                cur.execute("SELECT amount FROM ledger_entries WHERE event_id = %s AND bucket = 'held'", (ev["ref"],))
                row = cur.fetchone()
                if row:
                    entry(cur, ev, "held", -round(row[0] * 100))
            elif t == "REFUND":
                entry(cur, ev, "settled", -ev["amount_minor"])
            elif t == "CHARGEBACK":
                entry(cur, ev, "settled", -(ev["amount_minor"] + ev["fee_minor"]))
                cur.execute("UPDATE platform_fees SET total = total + %s WHERE currency = %s",
                            (ev["fee_minor"] / 100.0, ev["currency"]))
            elif t == "PAYOUT_REQUEST":
                amount = ev["amount_minor"] / 100.0
                if available(cur, ev["merchant_id"]) >= amount:
                    entry(cur, ev, "settled", -ev["amount_minor"])
                    cur.execute("INSERT INTO payouts (payout_id, merchant_id, amount, status) VALUES (%s, %s, %s, 'accepted')",
                                (ev["event_id"], ev["merchant_id"], amount))
                    cur.execute("INSERT INTO payout_outbox (payout_id) VALUES (%s)", (ev["event_id"],))
                else:
                    cur.execute("INSERT INTO payouts (payout_id, merchant_id, amount, status) VALUES (%s, %s, %s, 'rejected')",
                                (ev["event_id"], ev["merchant_id"], amount))
            else:
                raise ValueError(f"unknown event type {t}")
    finally:
        conn.close()


def handle(msg):
    apply(json.loads(msg.value()))
    consumer.commit(offsets=[TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)], asynchronous=True)


def balance_as_of(conn, merchant_id: str, as_of: str) -> dict:
    """Backs GET /v1/merchants/{id}/balance?as_of=..."""
    rows = conn.execute(
        "SELECT bucket, sum(amount) FROM ledger_entries "
        "WHERE merchant_id = %s AND event_time::date <= %s::date GROUP BY bucket",
        (merchant_id, as_of)).fetchall()
    return {bucket: round(total * 100) for bucket, total in rows}


def main():
    logging.basicConfig(level=logging.INFO)
    consumer.subscribe(["ledger-events"])
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        pool.submit(handle, msg)


if __name__ == "__main__":
    main()
