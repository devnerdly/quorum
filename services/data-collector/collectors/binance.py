"""Binance USD-M Futures collector for TRADIFI WTI perpetual (CLUSDT).

CLUSDT is a TRADIFI_PERPETUAL contract that tracks NYMEX WTI crude oil
front-month. Onboarded Feb 2026. Real volume, real orderbook, real
tick data — dramatically better than Yahoo CL=F for algorithmic use.

Public market data endpoints don't require API auth. The shared
BINANCE_API_KEY is reserved for future trading integration.

Endpoints used:
  GET  /fapi/v1/klines       — historical OHLCV bars
  GET  /fapi/v1/ticker/price — latest mark price (fallback)

Alternate symbol: BZUSDT for Brent perpetual (same schema, just
override BINANCE_SYMBOL in env).
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

_BASE = "https://fapi.binance.com"
_STREAM = "prices.brent"  # legacy stream name, carries CLUSDT WTI data now
_SOURCE = "binance"

# Maps Binance interval strings to internal timeframe labels.
# Binance supports: 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M
INTERVAL_MAP: dict[str, str] = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
    "1w": "1W",
}


def _symbol() -> str:
    return settings.binance_symbol or "CLUSDT"


def fetch_klines(interval: str = "1m", limit: int = 500) -> list[dict]:
    """Download klines (OHLCV candles) from Binance USD-M Futures REST API.

    Args:
        interval: Binance interval string (see INTERVAL_MAP keys).
        limit: 1..1500. Default 500 gives ~8h of 1-min bars.

    Returns:
        List of dicts with timestamp/source/timeframe/open/high/low/close/volume.
        Empty list on error.
    """
    timeframe = INTERVAL_MAP.get(interval)
    if timeframe is None:
        logger.warning("Unsupported Binance interval: %s", interval)
        return []

    params = {
        "symbol": _symbol(),
        "interval": interval,
        "limit": max(1, min(1500, int(limit))),
    }

    try:
        response = requests.get(f"{_BASE}/fapi/v1/klines", params=params, timeout=15)
        response.raise_for_status()
        raw = response.json()
    except Exception as exc:
        logger.error("Binance klines fetch failed (%s/%s): %s", params["symbol"], interval, exc)
        return []

    # Binance kline row format:
    # [0] openTime (ms), [1] open, [2] high, [3] low, [4] close, [5] volume,
    # [6] closeTime (ms), [7] quoteVolume, [8] trades, [9] takerBuyBase,
    # [10] takerBuyQuote, [11] ignore
    records: list[dict] = []
    for row in raw:
        try:
            records.append(
                {
                    "timestamp": datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
                    "source": _SOURCE,
                    "timeframe": timeframe,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                }
            )
        except (ValueError, IndexError, TypeError) as exc:
            logger.warning("Skipping malformed Binance kline row: %s (%s)", row, exc)
            continue

    logger.info(
        "Fetched %d %s klines from Binance (%s)",
        len(records), interval, params["symbol"],
    )
    return records


def collect_and_store(interval: str = "1m", limit: int = 500) -> None:
    """Fetch klines, upsert to DB, publish latest bar to Redis."""
    records = fetch_klines(interval=interval, limit=limit)
    if not records:
        return

    # Upsert by (source, timeframe, timestamp). Current (unfinished) bar is
    # refreshed on each poll until it closes.
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

    logger.info("Upserted %d OHLCV rows (binance %s)", len(records), interval)

    # Publish the most recent bar as a PriceEvent so downstream consumers
    # (analyzer, live_watch) react immediately.
    latest = records[-1]
    event = PriceEvent(**latest)
    publish(_STREAM, event.model_dump())
