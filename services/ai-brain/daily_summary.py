"""Daily P/L summary — fires once at ~22:00 UTC (5pm ET, WTI close).

Publishes a heartbeat_action event with the day's P/L for both personas
so the user gets a Telegram message summarizing the trading day.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from shared.account_manager import recompute_account_state
from shared.models.base import SessionLocal
from shared.models.campaigns import Campaign
from shared.redis_streams import publish

logger = logging.getLogger(__name__)

STREAM_POSITION = "position.event"
SUMMARY_HOUR_UTC = 22  # 5pm ET / 10pm UTC — roughly when WTI cash session ends


def _compute_daily_pnl(persona: str) -> dict:
    """Sum realized P/L from campaigns closed today + unrealized from open ones."""
    today_start = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    account = recompute_account_state(persona=persona)

    with SessionLocal() as session:
        closed_today = (
            session.query(Campaign)
            .filter(
                Campaign.persona == persona,
                Campaign.status != "open",
                Campaign.closed_at >= today_start,
            )
            .all()
        )

    realized_today = sum(float(c.realized_pnl or 0) for c in closed_today)
    trades_today = len(closed_today)
    wins_today = sum(1 for c in closed_today if (c.realized_pnl or 0) > 0)
    unrealized = account.get("unrealised_pnl", 0)
    equity = account.get("equity", 0)
    drawdown = account.get("account_drawdown_pct", 0)

    return {
        "persona": persona,
        "equity": round(equity, 2),
        "realized_today": round(realized_today, 2),
        "unrealized": round(unrealized, 2),
        "total_day_pnl": round(realized_today + unrealized, 2),
        "trades_closed_today": trades_today,
        "wins_today": wins_today,
        "drawdown_pct": round(drawdown, 1),
    }


def _publish_daily_summary() -> None:
    main = _compute_daily_pnl("main")
    scalper = _compute_daily_pnl("scalper")
    total_pnl = main["total_day_pnl"] + scalper["total_day_pnl"]
    total_trades = main["trades_closed_today"] + scalper["trades_closed_today"]

    lines = [
        f"Main: equity ${main['equity']:,.0f} | today {'+' if main['total_day_pnl'] >= 0 else ''}${main['total_day_pnl']:,.0f} "
        f"({main['trades_closed_today']} trades, {main['wins_today']}W) | dd {main['drawdown_pct']:+.1f}%",
        f"Scalper: equity ${scalper['equity']:,.0f} | today {'+' if scalper['total_day_pnl'] >= 0 else ''}${scalper['total_day_pnl']:,.0f} "
        f"({scalper['trades_closed_today']} trades, {scalper['wins_today']}W) | dd {scalper['drawdown_pct']:+.1f}%",
        f"Combined: {'+' if total_pnl >= 0 else ''}${total_pnl:,.0f} across {total_trades} trades",
    ]

    reason = "\n".join(lines)
    logger.info("Daily summary:\n%s", reason)

    try:
        publish(STREAM_POSITION, {
            "type": "heartbeat_action",
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "campaign_id": 0,
            "action": "daily_summary",
            "side": "",
            "reason": reason,
        })
    except Exception:
        logger.exception("Failed to publish daily summary")


def run_daily_summary_loop() -> None:
    """Background loop — fires the summary once per day at SUMMARY_HOUR_UTC."""
    logger.info("Daily summary worker started (fires at %02d:00 UTC)", SUMMARY_HOUR_UTC)
    time.sleep(90)  # let other workers boot

    last_fired_date = None

    while True:
        now = datetime.now(tz=timezone.utc)
        today = now.date()

        if now.hour >= SUMMARY_HOUR_UTC and last_fired_date != today:
            try:
                _publish_daily_summary()
                last_fired_date = today
            except Exception:
                logger.exception("Daily summary failed")

        time.sleep(60)  # check every minute
