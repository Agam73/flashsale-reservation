"""Small, dependency-free environment-variable config.

Mirrors internal/config's shape (a fallback value per variable, a
logged warning rather than a crash on a bad value) so anyone who's
already read the Go services' config.go recognizes this immediately.
Reads the *same* variable names as the Go services (KAFKA_BROKERS,
REDIS_ADDR) rather than inventing Python-flavored equivalents, since
one .env file is meant to configure this whole project, not one per
language.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger("risk-service.config")


def _string(key: str, fallback: str) -> str:
    value = os.environ.get(key)
    return value if value else fallback


def _int(key: str, fallback: int) -> int:
    value = os.environ.get(key)
    if not value:
        return fallback
    try:
        return int(value)
    except ValueError:
        logger.warning(
            "config: %s=%r is not a valid integer, using default %d",
            key,
            value,
            fallback,
        )
        return fallback


@dataclass(frozen=True)
class Settings:
    port: int
    redis_addr: str
    kafka_brokers: list[str]
    kafka_consumer_group: str

    # How long a cached risk score is trusted (redisx.SetRiskScore's
    # ttl argument, Go-side) before it's stale rather than wrong.
    risk_score_ttl_seconds: int

    # Sliding window (see risk_window.py) used to count recent joins
    # per user and per IP when computing a score.
    window_seconds: int

    # Heuristic thresholds -- see scoring.py's module doc for why these
    # are heuristics rather than a trained model, and what a count at
    # or above the threshold means.
    max_joins_per_user: int
    max_users_per_ip: int


def load() -> Settings:
    """Reads settings from the environment, the same way each Go
    service's main() does at startup -- called once, here, rather than
    scattering os.environ.get calls through the rest of the service."""
    return Settings(
        port=_int("RISK_SERVICE_PORT", 8085),
        redis_addr=_string("REDIS_ADDR", "localhost:6379"),
        kafka_brokers=_string("KAFKA_BROKERS", "localhost:9092").split(","),
        kafka_consumer_group=_string("RISK_KAFKA_CONSUMER_GROUP", "risk-service"),
        risk_score_ttl_seconds=_int("RISK_SCORE_TTL_SECONDS", 180),
        window_seconds=_int("RISK_WINDOW_SECONDS", 60),
        max_joins_per_user=_int("RISK_MAX_JOINS_PER_USER", 3),
        max_users_per_ip=_int("RISK_MAX_USERS_PER_IP", 5),
    )
