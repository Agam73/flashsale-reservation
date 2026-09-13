from __future__ import annotations

from datetime import datetime, timedelta, timezone

import risk_window


def test_extract_host_strips_ipv4_port():
    assert risk_window.extract_host("203.0.113.7:54321") == "203.0.113.7"


def test_extract_host_strips_bracketed_ipv6_port():
    assert risk_window.extract_host("[::1]:54321") == "::1"


def test_extract_host_falls_back_for_unexpected_format():
    assert risk_window.extract_host("not-a-host-port") == "not-a-host-port"


async def test_count_recent_joins_for_user_increments(redis_conn):
    now = datetime.now(timezone.utc)

    first = await risk_window.count_recent_joins_for_user(
        redis_conn, "alice", "item-1", now, window_seconds=60
    )
    assert first == 1

    second = await risk_window.count_recent_joins_for_user(
        redis_conn, "alice", "item-2", now + timedelta(seconds=1), window_seconds=60
    )
    assert second == 2


async def test_count_recent_joins_for_user_evicts_old_entries(redis_conn):
    now = datetime.now(timezone.utc)

    old = now - timedelta(seconds=120)
    await risk_window.count_recent_joins_for_user(redis_conn, "alice", "item-1", old, window_seconds=60)

    # A join well outside the 60s window shouldn't count toward a
    # join happening "now".
    count = await risk_window.count_recent_joins_for_user(
        redis_conn, "alice", "item-2", now, window_seconds=60
    )
    assert count == 1


async def test_count_recent_joins_is_per_user(redis_conn):
    now = datetime.now(timezone.utc)

    await risk_window.count_recent_joins_for_user(redis_conn, "alice", "item-1", now, window_seconds=60)
    bob_count = await risk_window.count_recent_joins_for_user(
        redis_conn, "bob", "item-1", now, window_seconds=60
    )
    assert bob_count == 1


async def test_count_recent_users_for_ip_counts_distinct_users(redis_conn):
    now = datetime.now(timezone.utc)
    ip = "203.0.113.7:1111"

    await risk_window.count_recent_users_for_ip(redis_conn, ip, "alice", now, window_seconds=60)
    await risk_window.count_recent_users_for_ip(redis_conn, ip, "bob", now, window_seconds=60)
    count = await risk_window.count_recent_users_for_ip(redis_conn, ip, "carol", now, window_seconds=60)

    assert count == 3


async def test_count_recent_users_for_ip_ignores_port_differences(redis_conn):
    # Same host, different ephemeral port -- should still be treated
    # as the same IP for grouping purposes.
    now = datetime.now(timezone.utc)

    await risk_window.count_recent_users_for_ip(
        redis_conn, "203.0.113.7:1111", "alice", now, window_seconds=60
    )
    count = await risk_window.count_recent_users_for_ip(
        redis_conn, "203.0.113.7:2222", "bob", now, window_seconds=60
    )
    assert count == 2


async def test_count_recent_users_for_ip_does_not_double_count_repeat_user(redis_conn):
    now = datetime.now(timezone.utc)
    ip = "203.0.113.7:1111"

    await risk_window.count_recent_users_for_ip(redis_conn, ip, "alice", now, window_seconds=60)
    count = await risk_window.count_recent_users_for_ip(
        redis_conn, ip, "alice", now + timedelta(seconds=1), window_seconds=60
    )
    assert count == 1
