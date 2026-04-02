"""IMF PortWatch port-congestion collector.

Fetches port throughput / congestion data from the IMF PortWatch API for key
crude-oil export and import terminals, then stores the results as
ShippingMetric rows.
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

_PORTWATCH_URL = "https://portwatch.imf.org/api/portcalls/timeseries"

# Key crude-oil ports to monitor (PortWatch uses UN/LOCODE identifiers)
_CRUDE_PORTS: dict[str, str] = {
    "AEJEA": "Jebel Ali (UAE)",          # major Gulf transshipment
    "SARAS": "Ras Tanura (Saudi Arabia)", # world's largest crude terminal
    "IRKHK": "Khark Island (Iran)",       # primary Iranian crude export
    "KWSAA": "Shuaiba (Kuwait)",
    "NLRTM": "Rotterdam (Netherlands)",   # main European import hub
    "USHOU": "Houston (USA)",             # key US Gulf hub
    "CNSHA": "Shanghai (China)",          # major Asian importer
    "JPYOK": "Yokohama (Japan)",
    "SGSIN": "Singapore",                 # largest bunkering/transshipment port
    "INPCN": "Port Chennai (India)",
}


def fetch_port_congestion() -> list[dict[str, Any]]:
    """Fetch recent port call counts for key crude ports from IMF PortWatch.

    Returns a list of dicts, each with keys:
        - ``locode``      : UN/LOCODE of the port
        - ``port_name``   : human-readable name
        - ``vessel_calls``: vessel call count for the latest available period
        - ``date``        : ISO date string of the observation
    """
    results: list[dict[str, Any]] = []

    for locode, port_name in _CRUDE_PORTS.items():
        try:
            params = {
                "locode": locode,
                "frequency": "weekly",
                "vessel_type": "tanker",
            }
            response = requests.get(_PORTWATCH_URL, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()

            # PortWatch returns {"data": [{"date": "...", "portcalls": N}, ...]}
            data_points: list[dict] = (
                payload.get("data", []) if isinstance(payload, dict) else payload
            )
            if not data_points:
                logger.debug("No data for port %s (%s)", locode, port_name)
                continue

            # Take the most recent observation
            latest = max(data_points, key=lambda r: r.get("date", ""))
            results.append(
                {
                    "locode": locode,
                    "port_name": port_name,
                    "vessel_calls": latest.get("portcalls") or latest.get("vessel_calls"),
                    "date": latest.get("date"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("PortWatch fetch failed for %s (%s): %s", locode, port_name, exc)

    logger.info("PortWatch: retrieved congestion data for %d ports", len(results))
    return results


def collect_and_store() -> None:
    """Fetch port-congestion data and persist as ShippingMetric rows."""
    port_data = fetch_port_congestion()
    if not port_data:
        logger.warning("No PortWatch data to store")
        return

    now = datetime.now(tz=timezone.utc)

    with SessionLocal() as session:
        for entry in port_data:
            locode = entry["locode"]
            calls = entry.get("vessel_calls")
            metric_name = f"portwatch_calls_{locode.lower()}"

            row = ShippingMetric(
                timestamp=now,
                metric_name=metric_name,
                value=float(calls) if calls is not None else None,
                details=json.dumps(
                    {
                        "source": "portwatch",
                        "port_name": entry.get("port_name"),
                        "locode": locode,
                        "observation_date": entry.get("date"),
                    }
                ),
            )
            session.add(row)

        session.commit()

    logger.info("Stored %d PortWatch ShippingMetric rows", len(port_data))
