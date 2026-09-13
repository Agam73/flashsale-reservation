"""Turns sliding-window counts into a risk score in [0, 1].

This is a heuristic, not a trained model, despite risk-service being
labeled a "FastAPI AI service" in the phase plan: there's no labeled
data anywhere in this project to train anything on, and a made-up
model would be worse than an honest, explainable heuristic that at
least says exactly why a score came out the way it did. Two signals,
combined by taking whichever is worse rather than averaging them
(averaging would let a severe single signal get diluted by an
unremarkable one, which is the wrong failure mode for a fraud signal):

- Same user joining a lot, fast, across items (scripted/repeated
  attempts from one identity).
- Many distinct users joining from the same IP host (one machine
  running many disposable identities -- a classic bot-farm pattern).

Swapping this out for an actual trained classifier later shouldn't
need to touch anything outside this one function: consumer.py only
depends on ``score(...)``'s signature, not on how it's computed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskSignals:
    """The raw counts scoring.py combines. Kept as a small struct
    (rather than passing two bare ints around) so a third signal can
    be added later without changing every call site's argument list."""

    user_joins_in_window: int
    distinct_users_for_ip_in_window: int


def _ramp(count: int, threshold: int) -> float:
    """Maps a count to [0, 1], treating a single occurrence as the
    normal, unremarkable case rather than as partial evidence of
    suspicion.

    A ramp of count/threshold would mean every legitimate buyer's very
    first join already scores 1/threshold before anything actually
    repeats -- wrong, since one join by one person from one IP is
    exactly what a real buyer looks like. Ramping from (count - 1)
    instead means count=1 always scores 0, and the score only starts
    climbing once a *second* occurrence shows up, reaching 1.0 exactly
    at threshold and staying there beyond it.

    threshold <= 1 is a valid (if extreme) config meaning "even one
    occurrence is maximally suspicious" -- handled directly rather
    than dividing by zero.
    """
    if threshold <= 1:
        return 1.0 if count >= 1 else 0.0
    return max(0.0, min((count - 1) / (threshold - 1), 1.0))


def score(signals: RiskSignals, *, max_joins_per_user: int, max_users_per_ip: int) -> float:
    """Returns a risk score in [0, 1]. 0 means nothing suspicious was
    observed; 1 means at least one signal hit its configured
    threshold. See _ramp for why a lone, first-time occurrence of
    either signal scores 0 rather than some baseline fraction.
    """
    if max_joins_per_user <= 0:
        raise ValueError(f"max_joins_per_user must be positive, got {max_joins_per_user}")
    if max_users_per_ip <= 0:
        raise ValueError(f"max_users_per_ip must be positive, got {max_users_per_ip}")

    user_component = _ramp(signals.user_joins_in_window, max_joins_per_user)
    ip_component = _ramp(signals.distinct_users_for_ip_in_window, max_users_per_ip)
    return max(user_component, ip_component)
