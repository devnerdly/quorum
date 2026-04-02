"""EIA (Energy Information Administration) weekly petroleum inventory collector."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.macro import MacroEIA
from shared.redis_streams import publish
from shared.schemas.events import MacroEvent

logger = logging.getLogger(__name__)

_EIA_URL = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
_STREAM = "macro.eia"

# EIA series IDs for the fields we want
_SERIES = {
    "WCRSTUS1": "crude_inventory_total",   # Crude oil stocks, total US (thousand barrels)
    "WCSSTUS1": "spr_inventory",           # SPR stocks (thousand barrels)
    "WCUOK1": "cushing_inventory",         # Cushing, OK crude stocks (thousand barrels)
}


def parse_eia_response(data: dict[str, Any]) -> dict[str, Any]:
    """Parse EIA API v2 JSON response.

    Expects the standard EIA v2 envelope::

        {"response": {"data": [{"period": "...", "value": ..., "series-description": ...}, ...]}}

    Returns a dict with the latest record values plus ``crude_inventory_change``
    (difference between the two most-recent records for the first series found).
    """
    records = data.get("response", {}).get("data", [])
    if not records:
        logger.warning("EIA response contained no data records")
        return {}

    # Group records by series ID
    by_series: dict[str, list[dict]] = {}
    for rec in records:
        sid = rec.get("series-description", rec.get("duoarea", "unknown"))
        series_key = rec.get("series", sid)
        by_series.setdefault(series_key, []).append(rec)

    result: dict[str, Any] = {}

    # Extract the latest value for each series
    for series_id, field_name in _SERIES.items():
        rows = by_series.get(series_id, [])
        if not rows:
            continue
        # Sort descending by period so index 0 is latest
        rows_sorted = sorted(rows, key=lambda r: r.get("period", ""), reverse=True)
        latest = rows_sorted[0]
        try:
            result[field_name] = float(latest["value"]) if latest.get("value") is not None else None
        except (TypeError, ValueError):
            result[field_name] = None

        # Compute change for crude inventory (first series = WCRSTUS1)
        if series_id == "WCRSTUS1" and len(rows_sorted) >= 2:
            try:
                val_latest = float(rows_sorted[0]["value"])
                val_prev = float(rows_sorted[1]["value"])
                result["crude_inventory_change"] = val_latest - val_prev
            except (TypeError, ValueError):
                result["crude_inventory_change"] = None

        # Capture period date from latest record of first series
        if "report_date" not in result:
            period_str = latest.get("period", "")
            try:
                result["report_date"] = datetime.strptime(period_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                result["report_date"] = None

    return result


def fetch_eia_inventories() -> dict[str, Any]:
    """Fetch the last 2 weekly inventory records from EIA API v2.

    Requires ``settings.eia_api_key`` to be set.

    Returns a parsed dict ready for storage (see :func:`parse_eia_response`).
    """
    params = {
        "api_key": settings.eia_api_key,
        "frequency": "weekly",
        "data[]": "value",
        "facets[series][]": list(_SERIES.keys()),
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 2 * len(_SERIES),  # 2 records per series
    }
    logger.info("Fetching EIA inventories from %s", _EIA_URL)
    response = requests.get(_EIA_URL, params=params, timeout=30)
    response.raise_for_status()
    return parse_eia_response(response.json())


def collect_and_store() -> None:
    """Fetch EIA inventory data, persist to DB, and publish a MacroEvent."""
    data = fetch_eia_inventories()
    if not data:
        logger.warning("No EIA data to store")
        return

    now = datetime.now(tz=timezone.utc)

    row = MacroEIA(
        timestamp=now,
        report_date=data.get("report_date"),
        crude_inventory_total=data.get("crude_inventory_total"),
        crude_inventory_change=data.get("crude_inventory_change"),
        spr_inventory=data.get("spr_inventory"),
        cushing_inventory=data.get("cushing_inventory"),
        crude_production=data.get("crude_production"),
        refinery_utilization=data.get("refinery_utilization"),
        crude_imports=data.get("crude_imports"),
        crude_exports=data.get("crude_exports"),
    )

    with SessionLocal() as session:
        session.add(row)
        session.commit()

    logger.info("Stored MacroEIA record (report_date=%s)", data.get("report_date"))

    event = MacroEvent(timestamp=now, dataset="eia", data=data)
    publish(_STREAM, event.model_dump())
    logger.info("Published MacroEvent to stream '%s'", _STREAM)
