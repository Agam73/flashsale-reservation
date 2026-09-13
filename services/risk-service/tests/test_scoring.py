from __future__ import annotations

import pytest

from scoring import RiskSignals, score


def test_score_is_zero_when_nothing_suspicious():
    signals = RiskSignals(user_joins_in_window=0, distinct_users_for_ip_in_window=0)
    assert score(signals, max_joins_per_user=3, max_users_per_ip=5) == 0.0


def test_score_ramps_up_linearly_toward_user_threshold():
    # count=1 is the normal case (score 0); the ramp only starts once
    # a second occurrence shows up. Halfway between "just repeated
    # once" and the threshold should score halfway, not 0 or 1.
    signals = RiskSignals(user_joins_in_window=3, distinct_users_for_ip_in_window=0)
    assert score(signals, max_joins_per_user=5, max_users_per_ip=100) == pytest.approx(0.5)


def test_score_is_zero_for_a_single_normal_join():
    # A lone join by one user from one IP is exactly what a real
    # buyer looks like -- it must not carry a nonzero baseline score.
    signals = RiskSignals(user_joins_in_window=1, distinct_users_for_ip_in_window=1)
    assert score(signals, max_joins_per_user=3, max_users_per_ip=5) == 0.0


def test_score_caps_at_one_past_the_threshold():
    signals = RiskSignals(user_joins_in_window=999, distinct_users_for_ip_in_window=0)
    assert score(signals, max_joins_per_user=3, max_users_per_ip=100) == 1.0


def test_score_takes_the_worse_of_the_two_signals():
    # user signal is mild (0.2), ip signal is severe (1.0) -- the
    # combined score should reflect the severe one, not an average
    # that would dilute it down to 0.6.
    signals = RiskSignals(user_joins_in_window=1, distinct_users_for_ip_in_window=10)
    result = score(signals, max_joins_per_user=5, max_users_per_ip=5)
    assert result == 1.0


def test_score_rejects_nonpositive_thresholds():
    signals = RiskSignals(user_joins_in_window=1, distinct_users_for_ip_in_window=1)
    with pytest.raises(ValueError):
        score(signals, max_joins_per_user=0, max_users_per_ip=5)
    with pytest.raises(ValueError):
        score(signals, max_joins_per_user=5, max_users_per_ip=-1)
