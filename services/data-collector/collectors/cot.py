"""CFTC Commitment of Traders (COT) collector for crude oil futures."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.macro import MacroCOT
from shared.redis_streams import publish
from shared.schemas.events import MacroEvent

logger = logging.getLogger(__name__)

# Nasdaq Data Link (formerly Quandl) COT dataset for WTI crude oil futures (067651)
_COT_URL = "https://data.nasdaq.com/api/v3/datasets/CFTC/067651_F_ALL.json"
_STREAM = "macro.cot"

# Column indices in the Nasdaq dataset (0-based, after the "Date" column at index 0)
# Columns (from CFTC/Quandl CFTC_F_ALL layout):
#   0: Date
#   1: Open Interest
#   2: Noncommercial Long
#   3: Noncommercial Short
#   4: Noncommercial Spreading
#   5: Commercial Long
#   6: Commercial Short
#   7: Total Long
#   8: Total Short
#   9: Nonreportable Long
#  10: Nonreportable Short
_COL_DATE = 0
_COL_OPEN_INTEREST = 1
_COL_NC_LONG = 2
_COL_NC_SHORT = 3
_COL_C_LONG = 5
_COL_C_SHORT = 6


def parse_cot_row(row: list[Any]) -> dict[str, Any]:
    """Extract COT fields from a single data row.

    Args:
        row: A list from the Nasdaq dataset ``data`` array.  The first element
             is the date string; remaining elements are numeric columns.

    Returns:
        Dict with keys: ``report_date``, ``commercial_long``,
        ``commercial_short``, ``non_commercial_long``,
        ``non_commercial_short``, ``open_interest``,
        ``commercial_net``, ``non_commercial_net``.
    """
    def _float(val: Any) -> float | None:
        try:
            return float(val) if val is not None else None
        except (TypeError, ValueError):
            return None

    try:
        report_date = datetime.strptime(str(row[_COL_DATE]), "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except (IndexError, ValueError):
        report_date = None

    c_long = _float(row[_COL_C_LONG]) if len(row) > _COL_C_LONG else None
    c_short = _float(row[_COL_C_SHORT]) if len(row) > _COL_C_SHORT else None
    nc_long = _float(row[_COL_NC_LONG]) if len(row) > _COL_NC_LONG else None
    nc_short = _float(row[_COL_NC_SHORT]) if len(row) > _COL_NC_SHORT else None
    open_interest = _float(row[_COL_OPEN_INTEREST]) if len(row) > _COL_OPEN_INTEREST else None

    commercial_net = (c_long - c_short) if c_long is not None and c_short is not None else None
    non_commercial_net = (nc_long - nc_short) if nc_long is not None and nc_short is not None else None

    return {
        "report_date": report_date,
        "commercial_long": c_long,
        "commercial_short": c_short,
        "non_commercial_long": nc_long,
        "non_commercial_short": nc_short,
        "open_interest": open_interest,
        "commercial_net": commercial_net,
        "non_commercial_net": non_commercial_net,
    }


def fetch_cot() -> dict[str, Any]:
    """Fetch the most recent COT report row from Nasdaq Data Link.

    Returns:
        Parsed dict from :func:`parse_cot_row` for the latest report.
    """
    params: dict[str, Any] = {
        "rows": 1,  # only the latest record
    }
    if settings.quandl_api_key:
        params["api_key"] = settings.quandl_api_key

    logger.info("Fetching COT data from %s", _COT_URL)
    response = requests.get(_COT_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    dataset = payload.get("dataset", {})
    data_rows = dataset.get("data", [])

    if not data_rows:
        logger.warning("COT response contained no data rows")
        return {}

    return parse_cot_row(data_rows[0])


def collect_and_store() -> None:
    """Fetch COT data, persist to DB, and publish a MacroEvent."""
    data = fetch_cot()
    if not data:
        logger.warning("No COT data to store")
        return

    now = datetime.now(tz=timezone.utc)

    row = MacroCOT(
        timestamp=now,
        report_date=data.get("report_date"),
        commercial_long=data.get("commercial_long"),
        commercial_short=data.get("commercial_short"),
        non_commercial_long=data.get("non_commercial_long"),
        non_commercial_short=data.get("non_commercial_short"),
        open_interest=data.get("open_interest"),
    )

    with SessionLocal() as session:
        session.add(row)
        session.commit()

    logger.info("Stored MacroCOT record (report_date=%s)", data.get("report_date"))

    event = MacroEvent(timestamp=now, dataset="cot", data=data)
    publish(_STREAM, event.model_dump())
    logger.info("Published MacroEvent to stream '%s'", _STREAM)
