"""risk-service: Phase 10's "FastAPI AI service".

Consumes waiting-room-api's BuyerJoined events (Phase 10, Go side --
see internal/kafkax/waitingroom_event.go) and writes a bot/scalper risk
score to Redis for each buyer who joins a waiting room. See
consumer.py for the consume loop and scoring.py for how a score is
actually computed (a heuristic, not a trained model -- see that
module's doc for why).

Nothing else in this project reads what this service writes yet.
Per the Phase 1 design decision, the risk model runs asynchronously
and is never called synchronously from checkout-api's hot path; this
phase only builds the producer (waiting-room-api), the scorer (this
service), and the cache format they share (Phase 9's
internal/redisx/risk.go / this file's redis_client.py). Consuming the
score to actually change a decision is future work, not scoped here.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Request
from redis.asyncio import Redis

import consumer
import redis_client
from config import Settings, load

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("risk-service.main")

settings: Settings = load()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    redis = Redis.from_url(f"redis://{settings.redis_addr}")
    app.state.redis = redis

    stop_event = asyncio.Event()
    consumer_task = asyncio.create_task(consumer.run(settings, redis, stop_event))

    logger.info(
        "risk-service: started (port=%d, kafka brokers=%s, window=%ds)",
        settings.port,
        settings.kafka_brokers,
        settings.window_seconds,
    )
    try:
        yield
    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(consumer_task, timeout=10)
        except asyncio.TimeoutError:
            logger.warning("risk-service: consumer task did not stop within 10s, cancelling")
            consumer_task.cancel()
        await redis.aclose()
        logger.info("risk-service: stopped")


app = FastAPI(title="risk-service", lifespan=lifespan)


def get_redis(request: Request) -> Redis:
    """A real dependency (not just `app.state.redis` inline in the
    handler) purely so tests can override it -- FastAPI's
    dependency_overrides lets a test swap this for a fixture-managed
    Redis client without the app's lifespan (and therefore the Kafka
    consumer) ever needing to start."""
    return request.app.state.redis


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "service": "risk-service"}


@app.get("/items/{item_id}/users/{user_id}/risk-score")
async def get_cached_risk_score(
    item_id: str, user_id: str, redis: Redis = Depends(get_redis)
) -> dict:
    """Read-only introspection endpoint for verifying this service
    end to end and for local debugging. checkout-api does not call
    this -- see this file's module doc for why -- so a 404 here just
    means "not yet scored, or the score expired," not that anything is
    broken."""
    score = await redis_client.get_risk_score(redis, item_id, user_id)
    if score is None:
        raise HTTPException(
            status_code=404,
            detail="no cached score for this item/user (not yet scored, or expired)",
        )
    return {"item_id": item_id, "user_id": user_id, "risk_score": score}
