"""JODI (Joint Organisations Data Initiative) oil statistics collector."""

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

# JODI World database — public CSV download
_JODI_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/jodi_oil_worlddb_csv.zip"
# Fallback direct CSV URL (JODI publishes data at multiple endpoints)
_JODI_CSV_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/jodi_oil_worlddb_csv.csv"
_STREAM = "macro.jodi"

# Columns expected in JODI CSV (standard JODI World format)
_COL_COUNTRY = "COUNTRY"
_COL_PRODUCT = "PRODUCT"
_COL_FLOW = "FLOW"
_COL_UNIT = "UNIT"
_COL_DATE = "DATE"
_COL_VALUE = "VALUE"


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
    """Download and parse the JODI World oil statistics CSV.

    Returns:
        List of parsed record dicts (see :func:`parse_jodi_csv`).
    """
    logger.info("Fetching JODI data from %s", _JODI_CSV_URL)
    response = requests.get(_JODI_CSV_URL, timeout=60)
    response.raise_for_status()
    return parse_jodi_csv(response.text)


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
