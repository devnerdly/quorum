"""Datalastic AIS tanker position collector.

Fetches tanker positions from the Datalastic API, classifies vessels by type
and DWT, infers operational status, and computes aggregate shipping metrics
(floating storage, chokepoint traffic, fleet counts) for storage in the DB.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import requests

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.shipping import ShippingMetric, ShippingPosition

logger = logging.getLogger(__name__)

_DATALASTIC_URL = "https://api.datalastic.com/api/v0/vessel_find"

# ---------------------------------------------------------------------------
# Tanker classification by DWT (deadweight tonnage in metric tons)
# ---------------------------------------------------------------------------

TANKER_CLASSES = {
    "VLCC": 200_000,      # Very Large Crude Carrier: > 200K DWT
    "Suezmax": 120_000,   # Suezmax: > 120K DWT
    "Aframax": 80_000,    # Aframax:  > 80K DWT
}

# Keywords in AIS vessel type names that indicate crude tankers
_TANKER_TYPE_KEYWORDS = ("crude", "vlcc", "suezmax", "aframax", "tanker")

# ---------------------------------------------------------------------------
# Chokepoint bounding boxes: {name: (lat_min, lat_max, lon_min, lon_max)}
# ---------------------------------------------------------------------------

CHOKEPOINTS: dict[str, tuple[float, float, float, float]] = {
    "hormuz":  (25.5, 27.0,  56.0,  57.5),   # Strait of Hormuz
    "suez":    (29.5, 31.5,  32.0,  33.0),   # Suez Canal
    "malacca": ( 1.0,  6.5, 100.0, 104.5),   # Strait of Malacca
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def classify_tanker(type_name: str | None, dwt: float | None) -> str | None:
    """Return the tanker class (VLCC / Suezmax / Aframax) or None.

    Classification is attempted first by DWT thresholds, then by matching
    keywords in *type_name*.  Returns ``None`` when the vessel does not fall
    into any recognised crude-tanker category.
    """
    if dwt is not None:
        if dwt > TANKER_CLASSES["VLCC"]:
            return "VLCC"
        if dwt > TANKER_CLASSES["Suezmax"]:
            return "Suezmax"
        if dwt > TANKER_CLASSES["Aframax"]:
            return "Aframax"

    if type_name:
        lower = type_name.lower()
        if "vlcc" in lower:
            return "VLCC"
        if "suezmax" in lower:
            return "Suezmax"
        if "aframax" in lower:
            return "Aframax"
        if any(kw in lower for kw in _TANKER_TYPE_KEYWORDS):
            # Generic crude tanker — return a broad label
            return "Tanker"

    return None


def infer_status(speed: float | None, nav_status: str | None) -> str:
    """Infer vessel operational status from AIS speed-over-ground and nav status.

    Rules:
    - anchored  : speed < 0.5 knots  OR nav_status contains "anchor"
    - laden     : 0.5 <= speed <= 10 knots  (slow steaming — carrying cargo)
    - ballast   : speed > 10 knots  (fast, running empty)
    - unknown   : speed is None and no conclusive nav_status
    """
    nav = (nav_status or "").lower()
    if "anchor" in nav or "moored" in nav:
        return "anchored"

    if speed is None:
        return "unknown"

    if speed < 0.5:
        return "anchored"
    if speed <= 10.0:
        return "laden"
    return "ballast"


def _in_chokepoint(lat: float | None, lon: float | None) -> str | None:
    """Return the name of the chokepoint the vessel is inside, or None."""
    if lat is None or lon is None:
        return None
    for name, (lat_min, lat_max, lon_min, lon_max) in CHOKEPOINTS.items():
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return None


def parse_vessel_data(raw: dict[str, Any]) -> dict[str, Any]:
    """Parse a single AIS vessel record from the Datalastic API response.

    Returns a normalised dict with keys that map directly onto
    :class:`ShippingPosition` columns plus derived fields.
    """
    # Datalastic field names (may differ slightly across API versions)
    lat = raw.get("lat") or raw.get("latitude")
    lon = raw.get("lon") or raw.get("longitude")
    speed = raw.get("speed") or raw.get("sog")
    nav_status = raw.get("nav_status") or raw.get("navigational_status")
    dwt = raw.get("dwt") or raw.get("deadweight")

    # Parse ETA string → datetime (best-effort)
    eta_str = raw.get("eta")
    eta: datetime | None = None
    if eta_str:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                eta = datetime.strptime(eta_str, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue

    vessel_type = raw.get("vessel_type") or raw.get("type_name") or raw.get("ship_type")
    tanker_class = classify_tanker(vessel_type, float(dwt) if dwt is not None else None)
    status = infer_status(
        float(speed) if speed is not None else None,
        nav_status,
    )
    chokepoint = _in_chokepoint(
        float(lat) if lat is not None else None,
        float(lon) if lon is not None else None,
    )

    return {
        "vessel_name": raw.get("name") or raw.get("vessel_name") or "UNKNOWN",
        "imo": str(raw["imo"]) if raw.get("imo") else None,
        "vessel_type": vessel_type,
        "tanker_class": tanker_class,
        "latitude": float(lat) if lat is not None else None,
        "longitude": float(lon) if lon is not None else None,
        "speed": float(speed) if speed is not None else None,
        "status": status,
        "destination": raw.get("destination"),
        "eta": eta,
        "chokepoint": chokepoint,
        "dwt": float(dwt) if dwt is not None else None,
    }


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------

def fetch_tanker_positions() -> list[dict[str, Any]]:
    """Fetch crude-tanker positions from the Datalastic vessel-find endpoint.

    Queries for crude / VLCC / Suezmax / Aframax vessel types.
    Returns a list of parsed vessel dicts (may be empty if the API key is
    missing or the response contains no matching vessels).
    """
    if not settings.datalastic_api_key:
        logger.warning("DATALASTIC_API_KEY not configured — skipping fetch")
        return []

    params = {
        "api-key": settings.datalastic_api_key,
        "type": "crude_oil_tanker",
    }

    logger.info("Fetching tanker positions from Datalastic …")
    response = requests.get(_DATALASTIC_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    # Datalastic wraps results in {"data": [...]} or returns a bare list
    vessels: list[dict] = payload.get("data", payload) if isinstance(payload, dict) else payload

    parsed = []
    for raw in vessels:
        try:
            parsed.append(parse_vessel_data(raw))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not parse vessel record %r: %s", raw.get("name"), exc)

    logger.info("Fetched and parsed %d tanker records", len(parsed))
    return parsed


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def compute_shipping_metrics(positions: list[dict[str, Any]]) -> dict[str, float]:
    """Derive aggregate shipping metrics from a list of parsed vessel dicts.

    Computed metrics
    ----------------
    floating_storage    : number of VLCCs/Suezmax that are anchored
    hormuz_traffic      : vessel count inside the Hormuz chokepoint
    suez_traffic        : vessel count inside the Suez chokepoint
    malacca_traffic     : vessel count inside the Malacca chokepoint
    total_vlcc_count    : total number of VLCCs in the dataset
    anchored_count      : total anchored vessels (any class)
    """
    floating_storage = 0
    hormuz_traffic = 0
    suez_traffic = 0
    malacca_traffic = 0
    total_vlcc_count = 0
    anchored_count = 0

    for pos in positions:
        tc = pos.get("tanker_class")
        status = pos.get("status")
        chokepoint = pos.get("chokepoint")

        if tc == "VLCC":
            total_vlcc_count += 1

        if status == "anchored":
            anchored_count += 1
            if tc in ("VLCC", "Suezmax"):
                floating_storage += 1

        if chokepoint == "hormuz":
            hormuz_traffic += 1
        elif chokepoint == "suez":
            suez_traffic += 1
        elif chokepoint == "malacca":
            malacca_traffic += 1

    return {
        "floating_storage": float(floating_storage),
        "hormuz_traffic": float(hormuz_traffic),
        "suez_traffic": float(suez_traffic),
        "malacca_traffic": float(malacca_traffic),
        "total_vlcc_count": float(total_vlcc_count),
        "anchored_count": float(anchored_count),
    }


# ---------------------------------------------------------------------------
# Collect & store
# ---------------------------------------------------------------------------

def collect_and_store() -> None:
    """Fetch AIS positions, store ShippingPosition rows and ShippingMetric rows.

    No Redis publish — the analyzer pulls shipping data directly from the DB.
    """
    positions = fetch_tanker_positions()
    if not positions:
        logger.warning("No tanker positions retrieved — nothing to store")
        return

    now = datetime.now(tz=timezone.utc)

    with SessionLocal() as session:
        # Persist individual vessel positions
        for pos in positions:
            row = ShippingPosition(
                timestamp=now,
                vessel_name=pos["vessel_name"],
                imo=pos.get("imo"),
                vessel_type=pos.get("vessel_type"),
                latitude=pos.get("latitude"),
                longitude=pos.get("longitude"),
                speed=pos.get("speed"),
                status=pos.get("status"),
                destination=pos.get("destination"),
                eta=pos.get("eta"),
            )
            session.add(row)

        # Compute and persist aggregate metrics
        metrics = compute_shipping_metrics(positions)
        for metric_name, value in metrics.items():
            row = ShippingMetric(
                timestamp=now,
                metric_name=metric_name,
                value=value,
                details=json.dumps({"source": "datalastic", "vessel_count": len(positions)}),
            )
            session.add(row)

        session.commit()

    logger.info(
        "Stored %d ShippingPosition rows and %d ShippingMetric rows",
        len(positions),
        len(metrics),
    )
