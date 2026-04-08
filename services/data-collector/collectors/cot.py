"""CFTC Commitment of Traders (COT) collector for Brent crude oil futures.

Scrapes the public CFTC weekly disaggregated futures-only report directly from
cftc.gov (no API key needed). The Quandl/Nasdaq endpoint that this used to hit
became paywalled, so we now parse the raw fixed-format text feed.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Any

import requests

from shared.models.base import SessionLocal
from shared.models.macro import MacroCOT
from shared.redis_streams import publish
from shared.schemas.events import MacroEvent

logger = logging.getLogger(__name__)

# Free public CFTC feed — futures only, "short format" CSV-like text dump.
# Updated weekly on Friday afternoon ET with prior Tuesday's positions.
_CFTC_URL = "https://www.cftc.gov/dea/newcot/deafut.txt"
_STREAM = "macro.cot"

# CFTC contract code for ICE Brent Crude futures (NYMEX-listed Brent Last Day).
# (WTI on ICE Europe = "067411", Brent Last Day on NYMEX = "06765T".)
_BRENT_CONTRACT_CODE = "06765T"
# Fallback: WTI light sweet crude on NYMEX (most liquid crude oil contract).
# Brent and WTI move in tight correlation so this is a fine proxy if Brent is missing.
_WTI_CONTRACT_CODE = "067651"

# Column indices in the cftc.gov fixed-format file (0-based, after the
# contract name field at index 0). The format has 95+ columns; we only need:
#   1: yymmdd date
#   2: YYYY-MM-DD date  ← we use this
#   3: CFTC contract code
#   7: Open Interest (Total)
#   8: Non-Commercial Long
#   9: Non-Commercial Short
#  10: Non-Commercial Spreading
#  11: Commercial Long
#  12: Commercial Short
_COL_DATE = 2
_COL_CONTRACT_CODE = 3
_COL_OPEN_INTEREST = 7
_COL_NC_LONG = 8
_COL_NC_SHORT = 9
_COL_C_LONG = 11
_COL_C_SHORT = 12


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def parse_cot_row(row: list[str]) -> dict[str, Any]:
    """Extract relevant COT fields from one cftc.gov row."""
    try:
        report_date = datetime.strptime(row[_COL_DATE], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (IndexError, ValueError):
        report_date = None

    c_long = _to_float(row[_COL_C_LONG]) if len(row) > _COL_C_LONG else None
    c_short = _to_float(row[_COL_C_SHORT]) if len(row) > _COL_C_SHORT else None
    nc_long = _to_float(row[_COL_NC_LONG]) if len(row) > _COL_NC_LONG else None
    nc_short = _to_float(row[_COL_NC_SHORT]) if len(row) > _COL_NC_SHORT else None
    open_interest = _to_float(row[_COL_OPEN_INTEREST]) if len(row) > _COL_OPEN_INTEREST else None

    commercial_net = (
        c_long - c_short if c_long is not None and c_short is not None else None
    )
    non_commercial_net = (
        nc_long - nc_short if nc_long is not None and nc_short is not None else None
    )

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
    """Download cftc.gov text feed and return Brent (or WTI fallback) row."""
    logger.info("Fetching CFTC COT data from %s", _CFTC_URL)
    response = requests.get(_CFTC_URL, timeout=60)
    response.raise_for_status()

    text = response.text
    reader = csv.reader(io.StringIO(text), quotechar='"', skipinitialspace=True)

    brent_row: dict[str, Any] | None = None
    wti_row: dict[str, Any] | None = None

    for row in reader:
        if len(row) <= _COL_CONTRACT_CODE:
            continue
        code = row[_COL_CONTRACT_CODE].strip()
        if code == _BRENT_CONTRACT_CODE and brent_row is None:
            brent_row = parse_cot_row(row)
        elif code == _WTI_CONTRACT_CODE and wti_row is None:
            wti_row = parse_cot_row(row)

    if brent_row:
        logger.info("Using Brent COT contract code %s", _BRENT_CONTRACT_CODE)
        return brent_row
    if wti_row:
        logger.warning(
            "Brent COT contract %s not found — falling back to WTI %s",
            _BRENT_CONTRACT_CODE,
            _WTI_CONTRACT_CODE,
        )
        return wti_row

    logger.warning("No matching crude oil rows found in CFTC feed")
    return {}


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

    logger.info(
        "Stored MacroCOT row (report_date=%s, nc_net=%s)",
        data.get("report_date"),
        data.get("non_commercial_net"),
    )

    event = MacroEvent(timestamp=now, dataset="cot", data=data)
    publish(_STREAM, event.model_dump())
    logger.info("Published MacroEvent to stream '%s'", _STREAM)
