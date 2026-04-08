"""IMF PortWatch port-traffic collector.

Pulls per-port tanker traffic counts from the public IMF PortWatch ArcGIS
FeatureServer. No API key required.

Endpoint discovered via the ArcGIS Open Data Hub:
    https://services8.arcgis.com/8KDV2PscG0fGIBii/arcgis/rest/services/Ports_(PortWatch)/FeatureServer/0
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import requests

from shared.models.base import SessionLocal
from shared.models.shipping import ShippingMetric

logger = logging.getLogger(__name__)

_PORTWATCH_URL = (
    "https://services8.arcgis.com/8KDV2PscG0fGIBii/arcgis/rest/services/"
    "Ports_(PortWatch)/FeatureServer/0/query"
)

# Key crude-oil ports we care about. PortWatch uses 5-char LOCODEs with a
# space between country prefix and port code (e.g. "NL RTM" not "NLRTM").
_CRUDE_PORTS: dict[str, str] = {
    "AE JEA": "Jebel Ali (UAE)",
    "SA RAS": "Ras Tanura (Saudi Arabia)",
    "IR KHK": "Khark Island (Iran)",
    "KW SAA": "Shuaiba (Kuwait)",
    "NL RTM": "Rotterdam (Netherlands)",
    "US HOU": "Houston (USA)",
    "CN SHA": "Shanghai (China)",
    "JP YOK": "Yokohama (Japan)",
    "SG SIN": "Singapore",
    "IN MAA": "Chennai (India)",
}


def fetch_port_congestion() -> list[dict[str, Any]]:
    """Fetch tanker counts for our key crude-oil ports from PortWatch.

    Uses one ArcGIS query with a LOCODE IN (...) filter — single round-trip.
    """
    locodes = "','".join(_CRUDE_PORTS.keys())
    params = {
        "where": f"LOCODE IN ('{locodes}')",
        "outFields": "LOCODE,portname,country,vessel_count_total,vessel_count_tanker,vessel_count_container,share_country_maritime_import,share_country_maritime_export",
        "f": "json",
        "resultRecordCount": 50,
    }

    logger.info("Fetching PortWatch port traffic …")
    response = requests.get(_PORTWATCH_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    features = payload.get("features", [])
    if not features:
        logger.warning("PortWatch returned no features")
        return []

    results: list[dict[str, Any]] = []
    for feat in features:
        attrs = feat.get("attributes", {}) or {}
        locode = attrs.get("LOCODE")
        if not locode:
            continue
        results.append(
            {
                "locode": locode,
                "port_name": _CRUDE_PORTS.get(locode, attrs.get("portname", locode)),
                "country": attrs.get("country"),
                "vessel_count_total": attrs.get("vessel_count_total"),
                "vessel_count_tanker": attrs.get("vessel_count_tanker"),
                "share_import": attrs.get("share_country_maritime_import"),
                "share_export": attrs.get("share_country_maritime_export"),
            }
        )

    logger.info("PortWatch: retrieved data for %d ports", len(results))
    return results


def collect_and_store() -> None:
    """Fetch port traffic and persist as ShippingMetric rows."""
    port_data = fetch_port_congestion()
    if not port_data:
        logger.warning("No PortWatch data to store")
        return

    now = datetime.now(tz=timezone.utc)

    with SessionLocal() as session:
        for entry in port_data:
            locode = entry["locode"].lower()
            tanker_calls = entry.get("vessel_count_tanker")
            metric_name = f"portwatch_tanker_{locode}"

            row = ShippingMetric(
                timestamp=now,
                metric_name=metric_name,
                value=float(tanker_calls) if tanker_calls is not None else None,
                details=json.dumps(
                    {
                        "source": "portwatch",
                        "port_name": entry.get("port_name"),
                        "locode": entry.get("locode"),
                        "country": entry.get("country"),
                        "vessel_count_total": entry.get("vessel_count_total"),
                        "share_import": entry.get("share_import"),
                        "share_export": entry.get("share_export"),
                    }
                ),
            )
            session.add(row)

        session.commit()

    logger.info("Stored %d PortWatch ShippingMetric rows", len(port_data))
