"""Plugin: alert management tools (set / list / cancel alerts).

Exposes PLUGIN_TOOLS (list of Anthropic tool schemas) and execute(name, input).
The orchestrator merges these into the main TOOLS list at startup.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anthropic tool schemas
# ---------------------------------------------------------------------------

PLUGIN_TOOLS = [
    {
        "name": "set_price_alert",
        "description": (
            "Create an alert that fires when Brent crosses a specific price in a given direction. "
            "The alert pushes to the user's Telegram."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "price": {"type": "number"},
                "direction": {"type": "string", "enum": ["above", "below"]},
                "message": {"type": "string"},
            },
            "required": ["price", "direction"],
        },
    },
    {
        "name": "set_keyword_watch",
        "description": (
            "Create an alert that fires when a keyword appears in a new @marketfeed digest or "
            "sentiment news item (case-insensitive substring match on the title/summary)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string"},
                "message": {"type": "string"},
            },
            "required": ["keyword"],
        },
    },
    {
        "name": "set_score_alert",
        "description": (
            "Create an alert that fires when an analysis score component crosses a threshold. "
            "Components: technical, fundamental, sentiment, shipping, unified. "
            "Direction: above, below, crosses."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "component": {
                    "type": "string",
                    "enum": ["technical", "fundamental", "sentiment", "shipping", "unified"],
                },
                "threshold": {"type": "number"},
                "direction": {
                    "type": "string",
                    "enum": ["above", "below", "crosses"],
                    "default": "above",
                },
                "message": {"type": "string"},
            },
            "required": ["component", "threshold"],
        },
    },
    {
        "name": "list_active_alerts",
        "description": "List all currently active alerts.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "cancel_alert",
        "description": "Cancel an active alert by id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "alert_id": {"type": "integer"},
            },
            "required": ["alert_id"],
        },
    },
]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def execute(name: str, input: dict) -> dict | None:
    """Return result dict, or None if this plugin does not handle *name*."""
    if name == "set_price_alert":
        return _set_price_alert(**input)
    if name == "set_keyword_watch":
        return _set_keyword_watch(**input)
    if name == "set_score_alert":
        return _set_score_alert(**input)
    if name == "list_active_alerts":
        return _list_active_alerts()
    if name == "cancel_alert":
        return _cancel_alert(**input)
    return None


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------

def _set_price_alert(price: float, direction: str, message: str | None = None) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.alerts import Alert

    now = datetime.now(tz=timezone.utc)
    with SessionLocal() as session:
        alert = Alert(
            created_at=now,
            kind="price",
            status="active",
            price_target=price,
            price_direction=direction,
            message=message,
            one_shot=True,
        )
        session.add(alert)
        session.commit()
        session.refresh(alert)
        alert_id = alert.id

    description = f"Brent price {direction} {price}"
    logger.info("Created price alert #%s: %s", alert_id, description)
    return {
        "alert_id": alert_id,
        "kind": "price",
        "status": "active",
        "description": description,
    }


def _set_keyword_watch(keyword: str, message: str | None = None) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.alerts import Alert

    now = datetime.now(tz=timezone.utc)
    with SessionLocal() as session:
        alert = Alert(
            created_at=now,
            kind="keyword",
            status="active",
            keyword=keyword,
            message=message,
            one_shot=True,
        )
        session.add(alert)
        session.commit()
        session.refresh(alert)
        alert_id = alert.id

    description = f'keyword watch: "{keyword}"'
    logger.info("Created keyword alert #%s: %s", alert_id, description)
    return {
        "alert_id": alert_id,
        "kind": "keyword",
        "status": "active",
        "description": description,
    }


def _set_score_alert(
    component: str,
    threshold: float,
    direction: str = "above",
    message: str | None = None,
) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.alerts import Alert

    now = datetime.now(tz=timezone.utc)
    with SessionLocal() as session:
        alert = Alert(
            created_at=now,
            kind="score",
            status="active",
            score_component=component,
            score_threshold=threshold,
            score_direction=direction,
            message=message,
            one_shot=True,
        )
        session.add(alert)
        session.commit()
        session.refresh(alert)
        alert_id = alert.id

    description = f"{component} score {direction} {threshold}"
    logger.info("Created score alert #%s: %s", alert_id, description)
    return {
        "alert_id": alert_id,
        "kind": "score",
        "status": "active",
        "description": description,
    }


def _list_active_alerts() -> dict:
    from shared.models.base import SessionLocal
    from shared.models.alerts import Alert

    with SessionLocal() as session:
        rows = session.query(Alert).filter(Alert.status == "active").all()
        alerts = []
        for a in rows:
            entry: dict = {
                "id": a.id,
                "kind": a.kind,
                "status": a.status,
                "created_at": a.created_at.isoformat(),
                "message": a.message,
                "one_shot": a.one_shot,
            }
            if a.kind == "price":
                entry["price_target"] = a.price_target
                entry["price_direction"] = a.price_direction
            elif a.kind == "keyword":
                entry["keyword"] = a.keyword
            elif a.kind == "score":
                entry["score_component"] = a.score_component
                entry["score_threshold"] = a.score_threshold
                entry["score_direction"] = a.score_direction
            alerts.append(entry)

    return {"count": len(alerts), "alerts": alerts}


def _cancel_alert(alert_id: int) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.alerts import Alert

    with SessionLocal() as session:
        alert = session.get(Alert, alert_id)
        if alert is None:
            return {"cancelled": False, "alert_id": alert_id, "error": "alert not found"}
        if alert.status != "active":
            return {
                "cancelled": False,
                "alert_id": alert_id,
                "error": f"alert is already {alert.status}",
            }
        alert.status = "cancelled"
        session.commit()

    logger.info("Cancelled alert #%s", alert_id)
    return {"cancelled": True, "alert_id": alert_id}
