"""FRED (Federal Reserve Economic Data) macro series collector."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.macro import MacroFRED
from shared.redis_streams import publish
from shared.schemas.events import MacroEvent

logger = logging.getLogger(__name__)

_FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
_STREAM = "macro.fred"

# FRED series to collect
FRED_SERIES: list[str] = [
    "DTWEXBGS",  # Nominal Broad U.S. Dollar Index
    "FEDFUNDS",  # Effective Federal Funds Rate
    "CPIAUCSL",  # Consumer Price Index for All Urban Consumers
    "T10Y2Y",    # 10-Year minus 2-Year Treasury Yield Spread
]


def parse_fred_series(data: dict[str, Any]) -> dict[str, Any] | None:
    """Parse FRED API observations response and return the latest observation.

    Args:
        data: Parsed JSON response from the FRED observations endpoint.

    Returns:
        Dict with keys ``date`` (datetime) and ``value`` (float | None),
        or ``None`` if no observations are present.
    """
    observations = data.get("observations", [])
    if not observations:
        logger.warning("FRED response contained no observations")
        return None

    # Observations are returned in ascending date order; take the last one
    latest = observations[-1]
    raw_value = latest.get("value", ".")
    try:
        value = float(raw_value) if raw_value != "." else None
    except (TypeError, ValueError):
        value = None

    try:
        date = datetime.strptime(latest["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (KeyError, ValueError):
        date = datetime.now(tz=timezone.utc)

    return {"date": date, "value": value}


def fetch_fred_series(series_id: str) -> dict[str, Any] | None:
    """Fetch the latest observation for a single FRED series.

    Args:
        series_id: FRED series identifier (e.g. ``"FEDFUNDS"``).

    Returns:
        Dict with ``date`` and ``value`` keys, or ``None`` on failure.
    """
    # FRED uses api_key as a query parameter.
    # We support a dedicated FRED key via FRED_API_KEY env var, falling back to quandl key.
    fred_key = getattr(settings, "fred_api_key", None) or settings.quandl_api_key or "no_key"
    params = {
        "series_id": series_id,
        "api_key": fred_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1,
        "observation_start": "2000-01-01",
    }

    logger.info("Fetching FRED series %s", series_id)
    response = requests.get(_FRED_URL, params=params, timeout=30)
    response.raise_for_status()
    return parse_fred_series(response.json())


def collect_and_store() -> None:
    """Fetch all configured FRED series, persist to DB, publish MacroEvents."""
    now = datetime.now(tz=timezone.utc)
    all_data: dict[str, Any] = {}

    with SessionLocal() as session:
        for series_id in FRED_SERIES:
            try:
                result = fetch_fred_series(series_id)
            except Exception as exc:
                logger.error("Failed to fetch FRED series %s: %s", series_id, exc)
                continue

            if result is None:
                continue

            row = MacroFRED(
                timestamp=now,
                series_id=series_id,
                value=result["value"],
            )
            session.add(row)
            all_data[series_id] = result["value"]
            logger.info("Stored MacroFRED %s = %s", series_id, result["value"])

        session.commit()

    event = MacroEvent(timestamp=now, dataset="fred", data=all_data)
    publish(_STREAM, event.model_dump())
    logger.info("Published MacroEvent to stream '%s' (%d series)", _STREAM, len(all_data))
