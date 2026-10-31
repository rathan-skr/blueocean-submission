"""Helpers for tests that need the real PostgreSQL / Redis from the compose stack (or a local one).

Set TEST_DATABASE_URL / TEST_REDIS_URL to point elsewhere. Tests are skipped if unreachable."""
import os

import psycopg
import pytest

ADMIN_URL = os.environ.get("TEST_ADMIN_DATABASE_URL", "postgresql://ledger@localhost:5432/postgres")
TEST_DB = os.environ.get("TEST_DB_NAME", "ledger_test")
TEST_URL = os.environ.get("TEST_DATABASE_URL", f"postgresql://ledger@localhost:5432/{TEST_DB}")
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


def fresh_db() -> str:
    try:
        with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=3) as c:
            c.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{TEST_DB}'")
            c.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
            c.execute(f"CREATE DATABASE {TEST_DB}")
    except psycopg.OperationalError as e:
        pytest.skip(f"no PostgreSQL for integration tests: {e}")
    from settle import config, db
    config.DATABASE_URL = TEST_URL
    db.bootstrap()
    return TEST_URL
