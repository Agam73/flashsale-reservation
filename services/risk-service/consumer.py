"""Consumes BuyerJoined events and writes a risk score for each.

Split into two pieces on purpose, the same way decision-service
separates internal/decision (pure logic) from cmd/decision-service's
consume loop: ``process_event`` does the actual work against a real
Redis connection and is unit-testable on its own; ``run`` is the thin
infinite loop around aiokafka that main.py starts as a background task
and cancels on shutdown. Nothing in this file needs Kafka to be
running in order to be tested -- only ``run`` touches
AIOKafkaConsumer, and this project doesn't have a broker available to
test against in every environment it's built in (the Go side has the
same gap; see docs/phase10.md).

A message that fails to decode is logged and skipped, not
dead-lettered: unlike decision-service's checkout-attempts topic,
nothing downstream depends on every BuyerJoined event being scored --
a buyer who never gets a score just reads as "unknown," which is
already the safe default (see redis_client.get_risk_score). Building a
DLQ for a best-effort side signal would be effort spent on the wrong
priority.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from redis.asyncio import Redis

import redis_client
import risk_window
from config import Settings
from schemas import BuyerJoined
from scoring import RiskSignals, score

logger = logging.getLogger("risk-service.consumer")

WAITING_ROOM_JOINS_TOPIC = "waiting-room-joins"


async def process_event(redis: Redis, settings: Settings, event: BuyerJoined) -> float:
    """Updates both sliding windows for this event, scores it, caches
    the result, and returns the score -- the one function this
    service's tests exercise directly against a real Redis."""
    user_count = await risk_window.count_recent_joins_for_user(
        redis, event.user_id, event.item_id, event.joined_at, settings.window_seconds
    )
    ip_count = await risk_window.count_recent_users_for_ip(
        redis, event.remote_addr, event.user_id, event.joined_at, settings.window_seconds
    )

    signals = RiskSignals(
        user_joins_in_window=user_count,
        distinct_users_for_ip_in_window=ip_count,
    )
    risk_score = score(
        signals,
        max_joins_per_user=settings.max_joins_per_user,
        max_users_per_ip=settings.max_users_per_ip,
    )

    await redis_client.set_risk_score(
        redis, event.item_id, event.user_id, risk_score, settings.risk_score_ttl_seconds
    )
    return risk_score


async def run(settings: Settings, redis: Redis, stop_event: asyncio.Event) -> None:
    """Consumes WAITING_ROOM_JOINS_TOPIC until stop_event is set. Owns
    its own AIOKafkaConsumer lifecycle (start/stop) so main.py's
    lifespan just has to create the task and set the event, not know
    anything about aiokafka directly.
    """
    consumer = AIOKafkaConsumer(
        WAITING_ROOM_JOINS_TOPIC,
        bootstrap_servers=settings.kafka_brokers,
        group_id=settings.kafka_consumer_group,
        value_deserializer=lambda v: v,  # decode ourselves, below, so a bad
        # message can be logged with its raw bytes rather than swallowed
        # by a deserializer exception aiokafka itself would raise.
    )
    await consumer.start()
    logger.info(
        "risk-service: consuming %s as group %s (brokers: %s)",
        WAITING_ROOM_JOINS_TOPIC,
        settings.kafka_consumer_group,
        settings.kafka_brokers,
    )
    try:
        while not stop_event.is_set():
            try:
                batch = await asyncio.wait_for(consumer.getmany(timeout_ms=1000), timeout=5)
            except asyncio.TimeoutError:
                continue

            for _tp, messages in batch.items():
                for message in messages:
                    await _handle_message(redis, settings, message.value)
    finally:
        await consumer.stop()
        logger.info("risk-service: consumer stopped")


async def _handle_message(redis: Redis, settings: Settings, raw_value: bytes) -> None:
    try:
        event = BuyerJoined.model_validate_json(raw_value)
    except ValidationError as exc:
        logger.warning("risk-service: skipping malformed BuyerJoined event: %s", exc)
        return

    try:
        risk_score = await process_event(redis, settings, event)
    except Exception:  # noqa: BLE001 -- one bad event must never kill the loop
        logger.exception(
            "risk-service: scoring failed for item=%s user=%s", event.item_id, event.user_id
        )
        return

    logger.info(
        "risk-service: scored item=%s user=%s score=%.3f",
        event.item_id,
        event.user_id,
        risk_score,
    )
