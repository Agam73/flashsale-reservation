"""Risk-score cache, wire-compatible with Go's internal/redisx/risk.go.

Same key format (``risk:{item_id}:{user_id}``) and the same contract:
a required TTL on writes, and a missing/expired key means "unknown,"
never an error -- so that nothing reading this cache (checkout-api,
eventually, though nothing does yet -- see this service's README-style
module doc in main.py) can be blocked or broken by a cache miss.

This service is the only writer for now. The value format
(``f"{score:.6f}"``, a fixed-point decimal string) is chosen so it
round-trips cleanly through Go's ``strconv.ParseFloat`` if anything on
that side ever reads a score directly, not just through this module.
"""

from __future__ import annotations

from redis.asyncio import Redis


def risk_score_key(item_id: str, user_id: str) -> str:
    return f"risk:{item_id}:{user_id}"


async def set_risk_score(
    redis: Redis, item_id: str, user_id: str, score: float, ttl_seconds: int
) -> None:
    if ttl_seconds <= 0:
        raise ValueError(f"risk score ttl must be positive, got {ttl_seconds}")
    if not 0.0 <= score <= 1.0:
        raise ValueError(f"risk score must be in [0, 1], got {score}")

    await redis.set(risk_score_key(item_id, user_id), f"{score:.6f}", ex=ttl_seconds)


async def get_risk_score(redis: Redis, item_id: str, user_id: str) -> float | None:
    """Returns the cached score, or None if nobody has scored this
    buyer yet, or the cached score expired. Never raises for a missing
    key -- see this module's doc."""
    value = await redis.get(risk_score_key(item_id, user_id))
    if value is None:
        return None
    return float(value)
