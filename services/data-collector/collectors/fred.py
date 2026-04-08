"""FRED (Federal Reserve Economic Data) macro series collector."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from sqlalchemy.dialects.postgresql import insert as pg_insert

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


def parse_fred_series(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse FRED API observations response and return all parsed points.

    Args:
        data: Parsed JSON response from the FRED observations endpoint.

    Returns:
        List of dicts with keys ``date`` (datetime) and ``value`` (float | None),
        sorted ascending by date. Empty list if no observations.
    """
    observations = data.get("observations", [])
    if not observations:
        logger.warning("FRED response contained no observations")
        return []

    parsed: list[dict[str, Any]] = []
    for obs in observations:
        raw_value = obs.get("value", ".")
        try:
            value = float(raw_value) if raw_value != "." else None
        except (TypeError, ValueError):
            value = None
        try:
            date = datetime.strptime(obs["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            continue
        if value is None:
            continue
        parsed.append({"date": date, "value": value})

    parsed.sort(key=lambda r: r["date"])
    return parsed


def fetch_fred_series(series_id: str, limit: int = 30) -> list[dict[str, Any]]:
    """Fetch recent observations for a single FRED series.

    Args:
        series_id: FRED series identifier (e.g. ``"FEDFUNDS"``).
        limit: Number of observations to fetch (newest first from API).

    Returns:
        List of {date, value} dicts sorted ascending by date.
    """
    fred_key = getattr(settings, "fred_api_key", "") or settings.quandl_api_key or "no_key"
    params = {
        "series_id": series_id,
        "api_key": fred_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }

    logger.info("Fetching FRED series %s", series_id)
    response = requests.get(_FRED_URL, params=params, timeout=30)
    response.raise_for_status()
    return parse_fred_series(response.json())


def collect_and_store() -> None:
    """Fetch all configured FRED series, persist all observations, publish MacroEvents."""
    now = datetime.now(tz=timezone.utc)
    summary: dict[str, Any] = {}

    with SessionLocal() as session:
        for series_id in FRED_SERIES:
            try:
                points = fetch_fred_series(series_id)
            except Exception as exc:
                logger.error("Failed to fetch FRED series %s: %s", series_id, exc)
                continue

            if not points:
                continue

            rows = [
                {"timestamp": p["date"], "series_id": series_id, "value": p["value"]}
                for p in points
            ]
            stmt = pg_insert(MacroFRED).values(rows).on_conflict_do_nothing(
                index_elements=["series_id", "timestamp"]
            )
            session.execute(stmt)

            latest = points[-1]
            summary[series_id] = latest["value"]
            logger.info("Upserted %d MacroFRED rows for %s (latest=%s)",
                        len(points), series_id, latest["value"])

        session.commit()

    event = MacroEvent(timestamp=now, dataset="fred", data=summary)
    publish(_STREAM, event.model_dump())
    logger.info("Published MacroEvent to stream '%s' (%d series)", _STREAM, len(summary))
