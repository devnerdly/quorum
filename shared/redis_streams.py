"""Redis Streams helpers for publishing and consuming events."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Generator

import redis

from shared.config import settings


def get_redis() -> redis.Redis:
    """Return a Redis connection using the configured REDIS_URL."""
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


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
            return

        for _stream, messages in results:
            for msg_id, fields in messages:
                raw = fields.get("json", "{}")
                data = json.loads(raw)
                yield msg_id, data
                r.xack(stream, group, msg_id)
