from __future__ import annotations

from datetime import datetime, timezone

import consumer
import redis_client
from config import Settings
from schemas import BuyerJoined


def make_settings(**overrides) -> Settings:
    defaults = dict(
        port=8085,
        redis_addr="localhost:6379",
        kafka_brokers=["localhost:9092"],
        kafka_consumer_group="risk-service-test",
        risk_score_ttl_seconds=60,
        window_seconds=60,
        max_joins_per_user=3,
        max_users_per_ip=5,
    )
    defaults.update(overrides)
    return Settings(**defaults)


async def test_process_event_writes_a_score(redis_conn):
    settings = make_settings()
    event = BuyerJoined(
        item_id="concert-ticket",
        user_id="alice",
        remote_addr="203.0.113.7:1111",
        joined_at=datetime.now(timezone.utc),
    )

    risk_score = await consumer.process_event(redis_conn, settings, event)
    assert risk_score == 0.0  # first-ever join, nothing suspicious yet

    cached = await redis_client.get_risk_score(redis_conn, "concert-ticket", "alice")
    assert cached == risk_score


async def test_process_event_flags_rapid_repeat_joins_from_one_user(redis_conn):
    settings = make_settings(max_joins_per_user=2, max_users_per_ip=100)
    base_time = datetime.now(timezone.utc)

    for i in range(3):
        event = BuyerJoined(
            item_id=f"item-{i}",
            user_id="bot-user",
            remote_addr="203.0.113.7:1111",
            joined_at=base_time,
        )
        risk_score = await consumer.process_event(redis_conn, settings, event)

    # Third join within the window hits the threshold (2) and exceeds
    # it -- should be capped at 1.0.
    assert risk_score == 1.0


async def test_process_event_flags_many_users_from_one_ip(redis_conn):
    settings = make_settings(max_joins_per_user=100, max_users_per_ip=2)
    base_time = datetime.now(timezone.utc)
    shared_ip = "203.0.113.7:1111"

    for user in ["alice", "bob", "carol"]:
        event = BuyerJoined(
            item_id="concert-ticket",
            user_id=user,
            remote_addr=shared_ip,
            joined_at=base_time,
        )
        risk_score = await consumer.process_event(redis_conn, settings, event)

    assert risk_score == 1.0


async def test_handle_message_skips_malformed_json_without_raising(redis_conn):
    settings = make_settings()
    # Should log and return, not raise -- a malformed message must
    # never take down the consume loop.
    await consumer._handle_message(redis_conn, settings, b"not json at all")


async def test_handle_message_processes_a_valid_event(redis_conn):
    settings = make_settings()
    event = BuyerJoined(
        item_id="concert-ticket",
        user_id="alice",
        remote_addr="203.0.113.7:1111",
        joined_at=datetime.now(timezone.utc),
    )
    raw = event.model_dump_json().encode("utf-8")

    await consumer._handle_message(redis_conn, settings, raw)

    cached = await redis_client.get_risk_score(redis_conn, "concert-ticket", "alice")
    assert cached is not None
