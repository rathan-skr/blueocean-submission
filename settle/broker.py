"""NATS JetStream connection and stream bootstrap."""
from __future__ import annotations

import asyncio
import logging

import nats
from nats.js.api import DiscardPolicy, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from . import config

log = logging.getLogger("broker")


async def connect(name: str):
    async def on_disc():
        log.warning("nats disconnected")

    async def on_reconn():
        log.info("nats reconnected")

    async def on_err(e):
        log.warning("nats error", extra={"error": repr(e)[:200]})

    delay = 0.5
    while True:
        try:
            nc = await nats.connect(config.NATS_URL, name=name, max_reconnect_attempts=-1, reconnect_time_wait=1,
                                    connect_timeout=3, allow_reconnect=True, pending_size=16 * 1024 * 1024,
                                    disconnected_cb=on_disc, reconnected_cb=on_reconn, error_cb=on_err)
            return nc, nc.jetstream(timeout=5)
        except Exception as e:
            log.warning("nats unavailable, retrying", extra={"error": repr(e)[:200]})
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5)


async def ensure_stream(js) -> None:
    """One stream, one subject per partition. File storage; R1 locally (R3 in production)."""
    cfg = StreamConfig(name=config.STREAM, subjects=[f"{config.SUBJECT_PREFIX}.*"],
                       retention=RetentionPolicy.LIMITS, storage=StorageType.FILE,
                       discard=DiscardPolicy.NEW,   # full -> reject publishes (backpressure), never drop unread data
                       max_age=float(config.STREAM_MAX_AGE_S), num_replicas=1)
    # NB: we deliberately do NOT set Nats-Msg-Id on publishes. Broker-side dedup would silently drop
    # a legitimate re-publish (after a reader reconnect) of a frame whose first copy a consumer had
    # discarded as out-of-order, leaving a permanent hole. Duplicates are removed by the ledger's
    # sequence guard instead, which is exact.
    while True:
        try:
            try:
                await js.stream_info(config.STREAM)
                return
            except NotFoundError:
                await js.add_stream(cfg)
                log.info("created stream", extra={"stream": config.STREAM, "partitions": config.PARTITIONS})
                return
        except Exception as e:
            if "already in use" in str(e):
                return
            log.warning("stream not ready, retrying", extra={"error": repr(e)[:200]})
            await asyncio.sleep(1)
