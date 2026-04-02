"""Alpha Vantage collector for Brent crude oil OHLCV data."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV
from shared.redis_streams import publish
from shared.schemas.events import PriceEvent

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.alphavantage.co/query"
_SYMBOL = "BZ"  # Brent crude on Alpha Vantage
_STREAM = "prices.brent"

# Maps interval strings to Alpha Vantage function + time_series key
_AV_CONFIG: dict[str, dict] = {
    "5min": {
        "function": "TIME_SERIES_INTRADAY",
        "interval": "5min",
        "series_key": "Time Series (5min)",
        "timeframe": "5min",
    },
    "1min": {
        "function": "TIME_SERIES_INTRADAY",
        "interval": "1min",
        "series_key": "Time Series (1min)",
        "timeframe": "1min",
    },
    "daily": {
        "function": "TIME_SERIES_DAILY",
        "interval": None,
        "series_key": "Time Series (Daily)",
        "timeframe": "1D",
    },
}


def fetch_brent_ohlcv_av(interval: str = "5min") -> list[dict]:
    """Fetch Brent crude OHLCV from Alpha Vantage REST API.

    Args:
        interval: One of "1min", "5min", "daily".

    Returns:
        List of dicts with keys: timestamp, source, timeframe, open, high,
        low, close, volume.
    """
    cfg = _AV_CONFIG.get(interval)
    if cfg is None:
        raise ValueError(f"Unsupported interval for Alpha Vantage: {interval!r}. "
                         f"Choose from {list(_AV_CONFIG)}")

    params: dict[str, str] = {
        "function": cfg["function"],
        "symbol": _SYMBOL,
        "apikey": settings.alpha_vantage_api_key,
        "outputsize": "compact",
        "datatype": "json",
    }
    if cfg["interval"] is not None:
        params["interval"] = cfg["interval"]

    response = requests.get(_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    series: dict = data.get(cfg["series_key"], {})
    if not series:
        logger.warning(
            "Alpha Vantage returned no data for interval=%s. Response keys: %s",
            interval,
            list(data.keys()),
        )
        return []

    records: list[dict] = []
    for ts_str, ohlcv in series.items():
        # AV timestamps are in format "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
        try:
            if " " in ts_str:
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            else:
                ts = datetime.strptime(ts_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning("Could not parse timestamp: %s", ts_str)
            continue

        records.append(
            {
                "timestamp": ts,
                "source": "alpha_vantage",
                "timeframe": cfg["timeframe"],
                "open": float(ohlcv["1. open"]),
                "high": float(ohlcv["2. high"]),
                "low": float(ohlcv["3. low"]),
                "close": float(ohlcv["4. close"]),
                "volume": float(ohlcv["5. volume"]) if "5. volume" in ohlcv else None,
            }
        )

    # Alpha Vantage returns newest first — sort ascending by timestamp
    records.sort(key=lambda r: r["timestamp"])
    logger.info("Fetched %d bars from Alpha Vantage (interval=%s)", len(records), interval)
    return records


def collect_and_store(interval: str = "5min") -> None:
    """Fetch OHLCV data from Alpha Vantage, persist to DB, publish to Redis."""
    records = fetch_brent_ohlcv_av(interval=interval)
    if not records:
        return

    with SessionLocal() as session:
        for rec in records:
            row = OHLCV(
                timestamp=rec["timestamp"],
                source=rec["source"],
                timeframe=rec["timeframe"],
                open=rec["open"],
                high=rec["high"],
                low=rec["low"],
                close=rec["close"],
                volume=rec["volume"],
            )
            session.add(row)
        session.commit()

    logger.info("Stored %d OHLCV rows from Alpha Vantage (interval=%s)", len(records), interval)

    # Publish the most recent bar as a PriceEvent
    latest = records[-1]
    event = PriceEvent(**latest)
    publish(_STREAM, event.model_dump())
    logger.info("Published PriceEvent to stream '%s' (alpha_vantage interval=%s)", _STREAM, interval)
