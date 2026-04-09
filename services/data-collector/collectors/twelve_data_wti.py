"""Twelve Data WTI collector — primary paid price feed.

Twelve Data Grow plan ($29/mo) gives us:
  - 55 req/min, no daily limit
  - Zero delay on WTI/USD (vs Yahoo's ~15-minute lag)
  - Reliable SLA with a real status page
  - Commodities + forex + stocks + ETFs in one API

We write into the shared OHLCV table with source="twelve" so it can
coexist with Yahoo (source="yahoo") and Binance (source="binance"). The
dashboard / analyzer / scoring read in priority order: twelve → yahoo
→ binance, so if the Twelve Data key is unset or its API is down the
system falls back to Yahoo without a code change.

Symbol: WTI/USD (Twelve Data's label for Crude Oil WTI Spot)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests
from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV
from shared.redis_streams import publish
from shared.schemas.events import PriceEvent

logger = logging.getLogger(__name__)

_BASE = "https://api.twelvedata.com"
_SYMBOL = "WTI/USD"
_SOURCE = "twelve"
_STREAM = "prices.brent"  # legacy stream name, carries WTI now

# Twelve Data interval strings → internal timeframe labels
_INTERVAL_MAP: dict[str, str] = {
    "1min": "1min",
    "5min": "5min",
    "15min": "15min",
    "30min": "30min",
    "1h": "1H",
    "4h": "4H",
    "1day": "1D",
    "1week": "1W",
}


def _parse_ts(datetime_str: str) -> datetime:
    """Parse Twelve Data's datetime string (e.g. '2026-04-09 21:53:00') as UTC."""
    # Try with space separator first, then ISO
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(datetime_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"unparseable datetime: {datetime_str!r}")


def fetch_time_series(interval: str = "1min", outputsize: int = 500) -> list[dict]:
    """Download WTI/USD bars from Twelve Data.

    outputsize: number of bars to return (max 5000 on Grow plan)
    """
    if not settings.twelve_api_key:
        logger.debug("twelve_data_wti: no API key configured — skipping")
        return []

    timeframe = _INTERVAL_MAP.get(interval)
    if timeframe is None:
        logger.warning("twelve_data_wti: unsupported interval %s", interval)
        return []

    try:
        r = requests.get(
            f"{_BASE}/time_series",
            params={
                "symbol": _SYMBOL,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": settings.twelve_api_key,
                "format": "JSON",
                "timezone": "UTC",
            },
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
    except Exception as exc:
        logger.error("twelve_data_wti fetch failed (%s): %s", interval, exc)
        return []

    if isinstance(payload, dict) and payload.get("status") == "error":
        logger.error(
            "twelve_data_wti API error (%s): %s",
            interval, payload.get("message", payload),
        )
        return []

    values = payload.get("values") or []
    if not values:
        logger.info("twelve_data_wti: no bars returned for %s", interval)
        return []

    records: list[dict] = []
    for row in values:
        try:
            records.append({
                "timestamp": _parse_ts(row["datetime"]),
                "source": _SOURCE,
                "timeframe": timeframe,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                # Commodity bars from Twelve Data often don't include volume;
                # leave it null and let downstream handle gracefully.
                "volume": float(row["volume"]) if row.get("volume") else None,
            })
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed Twelve Data row: %s (%s)", row, exc)

    logger.info(
        "Fetched %d Twelve Data WTI bars (interval=%s)", len(records), interval,
    )
    return records


def collect_and_store(interval: str = "1min", outputsize: int = 500) -> None:
    """Fetch WTI bars and upsert into OHLCV table, then publish latest."""
    records = fetch_time_series(interval=interval, outputsize=outputsize)
    if not records:
        return

    with SessionLocal() as session:
        stmt = pg_insert(OHLCV).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source", "timeframe", "timestamp"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
            },
        )
        session.execute(stmt)
        session.commit()

    logger.info("Upserted %d Twelve Data WTI rows (%s)", len(records), interval)

    # Publish the newest bar (records come newest-first from Twelve Data)
    latest = max(records, key=lambda r: r["timestamp"])
    try:
        event = PriceEvent(**latest)
        publish(_STREAM, event.model_dump())
    except Exception:
        logger.exception("Failed to publish Twelve Data PriceEvent")
