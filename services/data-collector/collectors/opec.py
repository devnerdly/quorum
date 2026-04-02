"""OPEC Monthly Oil Market Report (MOMR) collector."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests

from shared.models.base import SessionLocal
from shared.models.macro import MacroOPEC
from shared.redis_streams import publish
from shared.schemas.events import MacroEvent

logger = logging.getLogger(__name__)

# OPEC MOMR overview page — the raw HTML contains key production/demand figures
# that can be parsed by downstream AI agents.
_OPEC_URL = "https://www.opec.org/opec_web/en/publications/338.htm"
_STREAM = "macro.opec"

# Simple regex patterns to extract headline numbers from MOMR HTML/text
_RE_PRODUCTION = re.compile(
    r"OPEC[-\s]+(?:crude[-\s]+oil\s+)?production[^\d]*([\d]+\.[\d]+)\s*mb/d",
    re.IGNORECASE,
)
_RE_DEMAND = re.compile(
    r"world\s+oil\s+demand.*?average\s+([\d]+\.[\d]+)\s*mb/d",
    re.IGNORECASE | re.DOTALL,
)
_RE_NON_OPEC_SUPPLY = re.compile(
    r"non-opec\s+(?:liquids\s+)?supply.*?average\s+([\d]+\.[\d]+)\s*mb/d",
    re.IGNORECASE | re.DOTALL,
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; BrentTradingBot/1.0; +https://github.com/example/trading)"
    )
}


def _extract_float(pattern: re.Pattern, text: str) -> float | None:
    """Return the first float matched by *pattern* in *text*, or None."""
    match = pattern.search(text)
    if match:
        try:
            return float(match.group(1))
        except (IndexError, ValueError):
            pass
    return None


def fetch_opec_momr() -> dict[str, Any]:
    """Fetch the OPEC MOMR page and extract headline numbers plus raw text.

    Returns:
        Dict with keys: ``raw_text`` (str), ``total_production`` (float|None),
        ``demand_forecast`` (float|None), ``supply_forecast`` (float|None),
        ``report_date`` (datetime|None).
    """
    logger.info("Fetching OPEC MOMR page from %s", _OPEC_URL)
    response = requests.get(_OPEC_URL, headers=_HEADERS, timeout=60)
    response.raise_for_status()

    raw_text = response.text

    # Strip HTML tags for cleaner regex matching and AI ingestion
    clean_text = re.sub(r"<[^>]+>", " ", raw_text)
    clean_text = re.sub(r"\s{2,}", " ", clean_text).strip()

    total_production = _extract_float(_RE_PRODUCTION, clean_text)
    demand_forecast = _extract_float(_RE_DEMAND, clean_text)
    supply_forecast = _extract_float(_RE_NON_OPEC_SUPPLY, clean_text)

    # Try to detect the report month from the page (e.g. "April 2026")
    report_date: datetime | None = None
    date_match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
        clean_text,
    )
    if date_match:
        try:
            report_date = datetime.strptime(
                f"{date_match.group(1)} {date_match.group(2)}", "%B %Y"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    return {
        "raw_text": clean_text[:50_000],  # cap stored text size
        "total_production": total_production,
        "demand_forecast": demand_forecast,
        "supply_forecast": supply_forecast,
        "report_date": report_date,
    }


def collect_and_store() -> None:
    """Fetch OPEC MOMR data, persist to DB, and publish a MacroEvent."""
    data = fetch_opec_momr()

    now = datetime.now(tz=timezone.utc)

    row = MacroOPEC(
        timestamp=now,
        report_date=data.get("report_date"),
        total_production=data.get("total_production"),
        demand_forecast=data.get("demand_forecast"),
        supply_forecast=data.get("supply_forecast"),
        raw_text=data.get("raw_text"),
    )

    with SessionLocal() as session:
        session.add(row)
        session.commit()

    logger.info(
        "Stored MacroOPEC record (report_date=%s, production=%s mb/d)",
        data.get("report_date"),
        data.get("total_production"),
    )

    event = MacroEvent(
        timestamp=now,
        dataset="opec",
        data={
            "report_date": data.get("report_date").isoformat() if data.get("report_date") else None,
            "total_production": data.get("total_production"),
            "demand_forecast": data.get("demand_forecast"),
            "supply_forecast": data.get("supply_forecast"),
        },
    )
    publish(_STREAM, event.model_dump())
    logger.info("Published MacroEvent to stream '%s'", _STREAM)
