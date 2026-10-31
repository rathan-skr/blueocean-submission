"""PostgreSQL helpers: connection with retry, and idempotent schema bootstrap."""
from __future__ import annotations

import asyncio
import logging
import pathlib
import time

import psycopg

from . import config

log = logging.getLogger("db")
SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()


def connect(autocommit: bool = False, app: str = "settle") -> psycopg.Connection:
    """Blocking connect that waits for PostgreSQL to come back (restarts are part of the fault plan)."""
    delay = 0.5
    while True:
        try:
            conn = psycopg.connect(config.DATABASE_URL, autocommit=autocommit, application_name=app,
                                   connect_timeout=5)
            return conn
        except psycopg.OperationalError as e:
            log.warning("postgres unavailable, retrying", extra={"error": str(e).strip()[:200]})
            time.sleep(delay)
            delay = min(delay * 2, 5)


async def aconnect(autocommit: bool = True, app: str = "settle") -> psycopg.AsyncConnection:
    delay = 0.5
    while True:
        try:
            return await psycopg.AsyncConnection.connect(config.DATABASE_URL, autocommit=autocommit,
                                                         application_name=app, connect_timeout=5)
        except psycopg.OperationalError as e:
            log.warning("postgres unavailable, retrying", extra={"error": str(e).strip()[:200]})
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5)


def bootstrap() -> None:
    """Create the schema once, whichever service starts first (advisory lock serialises them)."""
    with connect(autocommit=True, app="bootstrap") as conn:
        conn.execute("SELECT pg_advisory_lock(424242)")
        try:
            conn.execute(SCHEMA)
            conn.execute(
                "INSERT INTO partitions (partition) SELECT g FROM generate_series(0, %s - 1) g ON CONFLICT DO NOTHING",
                (config.PARTITIONS,))
        finally:
            conn.execute("SELECT pg_advisory_unlock(424242)")
