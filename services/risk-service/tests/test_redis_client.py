from __future__ import annotations

import pytest

import redis_client


async def test_set_and_get_risk_score(redis_conn):
    await redis_client.set_risk_score(redis_conn, "item-1", "user-alice", 0.73, ttl_seconds=60)

    got = await redis_client.get_risk_score(redis_conn, "item-1", "user-alice")
    assert got == pytest.approx(0.73)


async def test_get_risk_score_not_found(redis_conn):
    got = await redis_client.get_risk_score(redis_conn, "item-1", "user-never-scored")
    assert got is None


async def test_risk_score_is_per_item_and_user(redis_conn):
    await redis_client.set_risk_score(redis_conn, "item-1", "user-alice", 0.2, ttl_seconds=60)
    await redis_client.set_risk_score(redis_conn, "item-1", "user-bob", 0.8, ttl_seconds=60)
    await redis_client.set_risk_score(redis_conn, "item-2", "user-alice", 0.4, ttl_seconds=60)

    assert await redis_client.get_risk_score(redis_conn, "item-1", "user-alice") == pytest.approx(0.2)
    assert await redis_client.get_risk_score(redis_conn, "item-1", "user-bob") == pytest.approx(0.8)
    assert await redis_client.get_risk_score(redis_conn, "item-2", "user-alice") == pytest.approx(0.4)


async def test_risk_score_expires(redis_conn):
    await redis_client.set_risk_score(redis_conn, "item-1", "user-alice", 0.9, ttl_seconds=1)

    # Confirm it's expired via TTL rather than sleeping in a test --
    # faster and not flaky under load.
    ttl = await redis_conn.ttl(redis_client.risk_score_key("item-1", "user-alice"))
    assert 0 < ttl <= 1


async def test_set_risk_score_rejects_bad_ttl(redis_conn):
    with pytest.raises(ValueError):
        await redis_client.set_risk_score(redis_conn, "item-1", "user-alice", 0.5, ttl_seconds=0)


@pytest.mark.parametrize("bad_score", [-0.1, 1.1])
async def test_set_risk_score_rejects_out_of_range_score(redis_conn, bad_score):
    with pytest.raises(ValueError):
        await redis_client.set_risk_score(redis_conn, "item-1", "user-alice", bad_score, ttl_seconds=60)
