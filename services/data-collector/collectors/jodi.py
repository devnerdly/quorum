"""JODI (Joint Organisations Data Initiative) oil statistics collector.

JODI Oil World Database publishes monthly statistics (production, demand,
exports, imports, stocks) per country. Data is available as annual CSV files
under /annual-csv/primary/{year}.csv. We fetch the current year file each run.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any

import requests

from shared.models.base import SessionLocal
from shared.models.macro import MacroJODI
from shared.redis_streams import publish
from shared.schemas.events import MacroEvent

logger = logging.getLogger(__name__)

# JODI publishes annual CSVs with the convention:
#   /annual-csv/primary/{year}.csv         (older years)
#   /annual-csv/primary/primaryyear{year}.csv  (current year)
_JODI_BASE = "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/primary"
_STREAM = "macro.jodi"

# Columns in the current JODI primary CSV format
_COL_COUNTRY = "REF_AREA"
_COL_DATE = "TIME_PERIOD"
_COL_PRODUCT = "ENERGY_PRODUCT"
_COL_FLOW = "FLOW_BREAKDOWN"
_COL_UNIT = "UNIT_MEASURE"
_COL_VALUE = "OBS_VALUE"


def _parse_date(date_str: str) -> datetime | None:
    """Parse a JODI date string (YYYY-MM or YYYYMM) to a UTC datetime."""
    for fmt in ("%Y-%m", "%Y%m"):
        try:
            return datetime.strptime(date_str.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def parse_jodi_csv(text: str) -> list[dict[str, Any]]:
    """Parse JODI CSV content into a list of record dicts.

    Args:
        text: Raw CSV text from the JODI download.

    Returns:
        List of dicts with keys: ``timestamp``, ``country``, ``product``,
        ``flow``, ``value``.
    """
    reader = csv.DictReader(io.StringIO(text))
    records: list[dict[str, Any]] = []

    for row in reader:
        # Normalise header names (strip whitespace, upper-case)
        normalised = {k.strip().upper(): v.strip() for k, v in row.items()}

        date_str = normalised.get(_COL_DATE, "")
        timestamp = _parse_date(date_str)
        if timestamp is None:
            continue

        raw_value = normalised.get(_COL_VALUE, "")
        try:
            value = float(raw_value) if raw_value not in ("", "x", "..") else None
        except ValueError:
            value = None

        records.append(
            {
                "timestamp": timestamp,
                "country": normalised.get(_COL_COUNTRY, ""),
                "product": normalised.get(_COL_PRODUCT, ""),
                "flow": normalised.get(_COL_FLOW, ""),
                "value": value,
            }
        )

    return records


def fetch_jodi() -> list[dict[str, Any]]:
    """Download and parse the current-year JODI primary CSV.

    Tries the "current year" naming convention first
    (``primaryyear{YYYY}.csv``), then falls back to plain ``{YYYY}.csv``,
    and finally to the previous year if neither works yet.
    """
    current_year = datetime.now(tz=timezone.utc).year
    candidates = [
        f"{_JODI_BASE}/primaryyear{current_year}.csv",
        f"{_JODI_BASE}/{current_year}.csv",
        f"{_JODI_BASE}/{current_year - 1}.csv",
    ]

    for url in candidates:
        try:
            logger.info("Trying JODI URL: %s", url)
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            # Some endpoints return HTML 404 page with 200 status — sanity check
            if "REF_AREA" not in response.text[:200]:
                logger.warning("URL %s returned non-CSV body, trying next", url)
                continue
            return parse_jodi_csv(response.text)
        except requests.HTTPError as exc:
            logger.warning("JODI URL %s returned %s", url, exc)
        except Exception:
            logger.exception("Unexpected error fetching JODI URL %s", url)

    logger.error("All JODI candidate URLs failed")
    return []


def collect_and_store() -> None:
    """Fetch JODI data, persist to DB, and publish a MacroEvent."""
    records = fetch_jodi()
    if not records:
        logger.warning("No JODI data to store")
        return

    now = datetime.now(tz=timezone.utc)

    with SessionLocal() as session:
        for rec in records:
            row = MacroJODI(
                timestamp=rec["timestamp"],
                country=rec["country"],
                product=rec["product"],
                flow=rec["flow"],
                value=rec["value"],
            )
            session.add(row)
        session.commit()

    logger.info("Stored %d MacroJODI records", len(records))

    event = MacroEvent(
        timestamp=now,
        dataset="jodi",
        data={"records_stored": len(records)},
    )
    publish(_STREAM, event.model_dump())
    logger.info("Published MacroEvent to stream '%s'", _STREAM)
