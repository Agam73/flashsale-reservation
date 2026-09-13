"""Sliding-window counters behind the heuristic in scoring.py.

Two questions, each answered with one Redis sorted set:

- How many times has this user_id joined a waiting room (any item) in
  the last window_seconds? -> risk_window:user:{user_id}
- How many distinct user_ids have joined from this IP in the last
  window_seconds? -> risk_window:ip:{host}

Both use the same shape: ZADD a new entry scored by the event's epoch
timestamp, ZREMRANGEBYSCORE to evict anything older than the window,
then ZCARD for the current count -- three commands batched into one
Redis pipeline per call, so this is one round trip either way. Each
key also gets an EXPIRE refreshed on every write (2x the window) so a
user or IP that goes quiet doesn't leave a key sitting in Redis
forever; nothing else ever explicitly deletes these.

Deliberately its own key namespace (risk_window:*), separate from
redis_client.py's risk:{item_id}:{user_id} score cache -- these are
working state scoring.py needs on every event, not the final result
anything else is meant to read.
"""

from __future__ import annotations

from datetime import datetime

from redis.asyncio import Redis


def extract_host(remote_addr: str) -> str:
    """Strips the port from a ``host:port`` address -- the format
    Go's ``r.RemoteAddr`` (and therefore BuyerJoined.remote_addr)
    uses -- handling bracketed IPv6 the way Go's own
    ``net.SplitHostPort`` would (``[::1]:12345`` -> ``::1``).

    Falls back to the input unchanged if it doesn't look like
    host:port at all: grouping by a slightly-off value is a minor,
    self-correcting inaccuracy in a heuristic signal, whereas raising
    here would mean one malformed address could stop a real event
    from ever being scored.
    """
    if remote_addr.startswith("["):
        end = remote_addr.find("]")
        if end != -1:
            return remote_addr[1:end]
        return remote_addr
    if remote_addr.count(":") == 1:
        return remote_addr.split(":", 1)[0]
    return remote_addr


async def _slide_and_count(
    redis: Redis, key: str, member: str, at: datetime, window_seconds: int
) -> int:
    now = at.timestamp()
    cutoff = now - window_seconds
    async with redis.pipeline(transaction=True) as pipe:
        pipe.zadd(key, {member: now})
        pipe.zremrangebyscore(key, "-inf", cutoff)
        pipe.expire(key, window_seconds * 2)
        pipe.zcard(key)
        results = await pipe.execute()
    return int(results[-1])


async def count_recent_joins_for_user(
    redis: Redis, user_id: str, item_id: str, joined_at: datetime, window_seconds: int
) -> int:
    """Records this join and returns how many joins (any item) this
    user_id has made in the trailing window_seconds, including this
    one. A high count is the "same identity retrying/scripting rapidly
    across sales" signal."""
    key = f"risk_window:user:{user_id}"
    # item_id alone could collide member-for-member if the same user
    # somehow joined the same item twice in the same instant; the
    # microsecond-resolution timestamp appended makes that practically
    # impossible without needing a separate random suffix.
    member = f"{item_id}|{joined_at.timestamp()!r}"
    return await _slide_and_count(redis, key, member, joined_at, window_seconds)


async def count_recent_users_for_ip(
    redis: Redis, remote_addr: str, user_id: str, joined_at: datetime, window_seconds: int
) -> int:
    """Records this join and returns how many distinct user_ids have
    joined from remote_addr's host in the trailing window_seconds,
    including this one. A high count is the "one machine, many
    disposable identities" bot-farm signal. Re-adding the same user_id
    from the same host just refreshes their timestamp -- sorted set
    members are unique, so repeats don't inflate the distinct count.
    """
    key = f"risk_window:ip:{extract_host(remote_addr)}"
    return await _slide_and_count(redis, key, user_id, joined_at, window_seconds)
