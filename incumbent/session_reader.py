"""
BOSP session reader: holds the single gateway session and publishes every ledger event to Kafka
(topic `ledger-events`), from where the ledger consumers apply them.

Run one replica per AZ; LeaderElector makes sure only one of them is connected at a time.
"""
import asyncio
import json
import logging
import os

import redis
from confluent_kafka import Producer

from leader import LeaderElector

log = logging.getLogger("reader")

GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "gateway")
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "7100"))
SECRET = os.environ["GATEWAY_SECRET"]
CREDIT_WINDOW = 2_000

r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
producer = Producer({
    "bootstrap.servers": os.environ.get("KAFKA", "kafka:9092"),
    "acks": "1",
    "linger.ms": 5,
    "enable.idempotence": False,
    "max.in.flight.requests.per.connection": 5,
    "retries": 10,
})


def encode(msg: dict) -> bytes:
    body = json.dumps(msg).encode()
    return len(body).to_bytes(4, "big") + body


async def read_frame(reader: asyncio.StreamReader) -> dict:
    header = await reader.readexactly(4)
    length = int.from_bytes(header, "big")
    body = await reader.read(length)
    return json.loads(body)


def checkpoint_key(channel: str) -> str:
    return f"bosp:checkpoint:{channel}"


def load_resume(channels) -> dict:
    out = {}
    for ch in channels:
        v = r.get(checkpoint_key(ch))
        if v:
            out[ch] = int(v)
    return out


def publish(event: dict):
    producer.produce("ledger-events", key=event["event_id"], value=json.dumps(event))
    producer.poll(0)


async def run_session(elector: LeaderElector):
    reader, writer = await asyncio.open_connection(GATEWAY_HOST, GATEWAY_PORT)
    known_channels = r.smembers("bosp:channels") or set()
    writer.write(encode({"type": "LOGON", "client_id": "bo-ledger", "secret": SECRET,
                         "resume": load_resume(known_channels)}))
    await writer.drain()
    ack = await read_frame(reader)
    if ack["type"] != "LOGON_ACK":
        raise RuntimeError(f"logon failed: {ack}")
    r.sadd("bosp:channels", *ack["channels"])
    resume = load_resume(ack["channels"])
    expected = {ch: resume.get(ch, 0) + 1 for ch in ack["channels"]}
    used = {ch: 0 for ch in ack["channels"]}
    for ch in ack["channels"]:
        writer.write(encode({"type": "CREDIT", "channel": ch, "n": CREDIT_WINDOW}))
    await writer.drain()
    log.info("session up, epoch=%s", ack["session_epoch"])

    while elector.is_leader:
        try:
            msg = await read_frame(reader)
        except json.JSONDecodeError:
            log.warning("corrupt frame from gateway, skipping")
            continue
        t = msg["type"]

        if t == "TEST_REQUEST":
            writer.write(encode({"type": "HEARTBEAT"}))
        elif t == "HEARTBEAT":
            writer.write(encode({"type": "HEARTBEAT"}))
        elif t == "LOGOUT":
            log.warning("gateway logged us out: %s", msg.get("reason"))
            return
        elif t in ("DATA", "NOTICE", "GAP_FILL"):
            ch, seq = msg["channel"], msg["seq"]
            used[ch] += 1
            if msg.get("poss_dup"):
                # already delivered to us earlier - don't publish twice
                pass
            elif t == "GAP_FILL":
                expected[ch] = msg["new_seq"]
            else:
                if seq != expected[ch]:
                    log.warning("gap on %s: expected %s got %s, requesting resend", ch, expected[ch], seq)
                    writer.write(encode({"type": "RESEND", "channel": ch, "from_seq": expected[ch]}))
                if t == "DATA":
                    publish(msg["event"])
                r.set(checkpoint_key(ch), seq)
                expected[ch] = seq + 1
            if used[ch] >= CREDIT_WINDOW // 2:
                writer.write(encode({"type": "CREDIT", "channel": ch, "n": used[ch]}))
                used[ch] = 0
        await writer.drain()

    writer.close()


async def main():
    logging.basicConfig(level=logging.INFO)
    elector = LeaderElector(r)
    elector.start()
    while True:
        try:
            await run_session(elector)
        except (ConnectionError, asyncio.IncompleteReadError) as e:
            log.warning("session dropped: %s", e)
        producer.flush(10)
        await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main())
