"""Shared fixtures. redis_conn is the Python-side equivalent of the Go
tests' testClient/testRedis helpers: connect to the same local Redis
those target, skip cleanly (not fail) if it isn't reachable, and start
each test from a clean database.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from redis.asyncio import Redis


@pytest_asyncio.fixture
async def redis_conn():
    client = Redis.from_url("redis://localhost:6379")
    try:
        await client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"skipping: no local Redis available: {exc}")

    await client.flushdb()
    yield client
    await client.aclose()
