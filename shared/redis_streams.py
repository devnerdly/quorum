"""Redis Streams helpers for publishing and consuming events."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Generator

import redis

from shared.config import settings


_pool: redis.ConnectionPool | None = None


def get_redis() -> redis.Redis:
    """Return a Redis connection from the shared connection pool."""
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool.from_url(
            settings.redis_url, decode_responses=True, max_connections=20
        )
    return redis.Redis(connection_pool=_pool)


def _serialize(obj: Any) -> Any:
    """Recursively convert values that JSON cannot serialise by default."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    return obj


def publish(stream: str, data: dict[str, Any]) -> str:
    """Serialise *data* and XADD it to *stream*.

    Returns the Redis message ID.
    """
    r = get_redis()
    payload = {"json": json.dumps(_serialize(data))}
    return r.xadd(stream, payload)


def subscribe(
    stream: str,
    group: str,
    consumer: str,
    count: int = 10,
    block: int = 5000,
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    """Consume messages from *stream* via a consumer group.

    Automatically creates the consumer group if it does not exist (starting
    from the very first message — ID ``0``).

    Yields ``(message_id, parsed_dict)`` tuples and ACKs each message after
    yielding.
    """
    r = get_redis()

    # Ensure the consumer group exists.
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    while True:
        results = r.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block,
        )
        if not results:
            continue

        for _stream, messages in results:
            for msg_id, fields in messages:
                try:
                    raw = fields.get("json", "{}")
                    data = json.loads(raw)
                    r.xack(stream, group, msg_id)  # ack first — at-most-once
                except Exception:
                    # Couldn't even parse, ack and skip
                    r.xack(stream, group, msg_id)
                    continue
                yield msg_id, data
