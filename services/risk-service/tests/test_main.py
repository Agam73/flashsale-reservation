from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

import main
import redis_client


def make_client(redis_conn) -> AsyncClient:
    """Builds an HTTP client against the FastAPI app with get_redis
    overridden to the test fixture's connection -- this deliberately
    never triggers `main.app`'s real lifespan (and therefore never
    starts the Kafka consumer), since no broker is available in this
    environment. See main.get_redis's doc for why the dependency
    exists at all."""
    main.app.dependency_overrides[main.get_redis] = lambda: redis_conn
    transport = ASGITransport(app=main.app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    main.app.dependency_overrides.clear()


async def test_healthz():
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


async def test_get_cached_risk_score_returns_404_when_unscored(redis_conn):
    async with make_client(redis_conn) as client:
        resp = await client.get("/items/concert-ticket/users/alice/risk-score")
    assert resp.status_code == 404


async def test_get_cached_risk_score_returns_the_cached_value(redis_conn):
    await redis_client.set_risk_score(redis_conn, "concert-ticket", "alice", 0.42, ttl_seconds=60)

    async with make_client(redis_conn) as client:
        resp = await client.get("/items/concert-ticket/users/alice/risk-score")

    assert resp.status_code == 200
    body = resp.json()
    assert body["item_id"] == "concert-ticket"
    assert body["user_id"] == "alice"
    assert body["risk_score"] == pytest.approx(0.42)
