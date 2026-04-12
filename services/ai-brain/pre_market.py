"""Pre-market preparation — fires ~1h before market open (Sunday 21:00 UTC).

Gathers weekend news, checks open positions, builds a situational brief,
and sends it to Telegram so the user (and the bot) is prepared for the
market open at 22:00 UTC Sunday.

Also wakes up the heartbeat early and arms the hot window so TP/SL checks
run at 30s cadence from the moment the first tick arrives. This ensures
gap risk is caught immediately on the open.

Runs as a daemon thread in ai-brain alongside the heartbeat.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from anthropic import Anthropic

from shared.config import settings
from shared.account_manager import recompute_account_state
from shared.models.base import SessionLocal
from shared.models.campaigns import Campaign
from shared.models.knowledge import KnowledgeSummary
from shared.models.ohlcv import OHLCV
from shared.models.signals import AnalysisScore
from shared.position_manager import compute_campaign_state
from shared.redis_streams import publish
from shared.llm_usage import record_anthropic_call

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"  # Sonnet is enough for a brief — save Opus for live management
STREAM_POSITION = "position.event"

# Fire the prep at Sunday 21:00 UTC (1h before open)
PRE_MARKET_HOUR_UTC = 21
PRE_MARKET_WEEKDAY = 6  # Sunday


def _gather_pre_market_context() -> dict:
    """Gather everything Opus needs to write the pre-market brief."""
    # Open positions
    open_camps = []
    with SessionLocal() as session:
        camps = (
            session.query(Campaign)
            .filter(Campaign.status == "open")
            .all()
        )
        for c in camps:
            state = compute_campaign_state(c.id)
            if state:
                open_camps.append({
                    "id": c.id,
                    "persona": c.persona,
                    "side": c.side,
                    "entry_price": state.get("avg_entry_price"),
                    "take_profit": c.take_profit,
                    "stop_loss": c.stop_loss,
                    "layers": state.get("layers_used"),
                    "total_margin": state.get("total_margin"),
                })

    # Last known price (will be stale — that's OK, we note the staleness)
    last_price = None
    last_price_at = None
    with SessionLocal() as session:
        row = (
            session.query(OHLCV)
            .filter(OHLCV.timeframe == "1min", OHLCV.source == "twelve")
            .order_by(OHLCV.timestamp.desc())
            .first()
        )
        if row:
            last_price = float(row.close)
            last_price_at = row.timestamp.isoformat()

    # Weekend news (last 72h to catch Friday evening + weekend developments)
    news = []
    since = datetime.now(tz=timezone.utc) - timedelta(hours=72)
    with SessionLocal() as session:
        rows = (
            session.query(KnowledgeSummary)
            .filter(KnowledgeSummary.timestamp >= since)
            .order_by(KnowledgeSummary.timestamp.desc())
            .limit(10)
            .all()
        )
        for r in rows:
            news.append({
                "ts": r.timestamp.isoformat(),
                "sentiment": r.sentiment_label,
                "score": r.sentiment_score,
                "summary": (r.summary or "")[:400],
            })

    # Latest scores (from Friday)
    scores = None
    with SessionLocal() as session:
        row = (
            session.query(AnalysisScore)
            .order_by(AnalysisScore.timestamp.desc())
            .first()
        )
        if row:
            scores = {
                "unified": row.unified_score,
                "technical": row.technical_score,
                "sentiment": row.sentiment_score,
                "timestamp": row.timestamp.isoformat(),
            }

    # Account state
    main_acc = recompute_account_state("main")
    scalper_acc = recompute_account_state("scalper")

    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "market_status": "CLOSED — opens in ~1 hour (Sunday 22:00 UTC)",
        "last_price": last_price,
        "last_price_at": last_price_at,
        "open_campaigns": open_camps,
        "weekend_news": news,
        "friday_scores": scores,
        "main_account": {
            "equity": main_acc.get("equity"),
            "drawdown": main_acc.get("account_drawdown_pct"),
        },
        "scalper_account": {
            "equity": scalper_acc.get("equity"),
            "drawdown": scalper_acc.get("account_drawdown_pct"),
        },
    }


def _generate_brief(context: dict) -> str:
    """Ask Sonnet to write a pre-market preparation brief."""
    client = Anthropic(api_key=settings.anthropic_api_key)

    system = (
        "You are a WTI crude oil trading assistant preparing a pre-market brief. "
        "The market opens in about 1 hour (Sunday 22:00 UTC). Your job is to:\n"
        "1. Summarize what happened over the weekend (news, developments)\n"
        "2. Analyze risks to any open positions based on the weekend news\n"
        "3. Flag specific gap risk scenarios (e.g. 'if oil gaps above $X, your SHORT SL triggers')\n"
        "4. Recommend whether to tighten/widen SL on open positions BEFORE the open\n"
        "5. Note what events/catalysts to watch in the first hour of trading\n\n"
        "Be direct, specific, cite price levels. This goes to Telegram so keep it under 2000 chars. "
        "Always reply in English."
    )

    user_prompt = (
        "## Pre-Market Context\n"
        f"{json.dumps(context, indent=2, default=str)}\n\n"
        "Write the pre-market brief now."
    )

    t0 = time.time()
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1000,
            system=system,
            messages=[{"role": "user", "content": user_prompt}],
        )
        record_anthropic_call(
            call_site="pre_market.sonnet",
            model=MODEL,
            usage=response.usage,
            duration_ms=(time.time() - t0) * 1000,
        )
        return response.content[0].text.strip()
    except Exception:
        logger.exception("Pre-market brief generation failed")
        return "Pre-market brief generation failed. Check open positions manually."


def run_pre_market_prep() -> None:
    """Fire the pre-market preparation brief. Called once per Sunday."""
    logger.info("Running pre-market preparation...")

    context = _gather_pre_market_context()
    brief = _generate_brief(context)

    # Publish to Telegram
    try:
        publish(STREAM_POSITION, {
            "type": "heartbeat_action",
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "campaign_id": 0,
            "action": "pre_market_brief",
            "side": "",
            "reason": brief,
        })
        logger.info("Pre-market brief published to Telegram")
    except Exception:
        logger.exception("Failed to publish pre-market brief")

    # Arm the hot window so the heartbeat starts TP/SL checks at 30s
    # cadence as soon as the first fresh price arrives on market open
    try:
        from shared.heartbeat_hot import arm_hot_window
        arm_hot_window(duration_seconds=10 * 60, reason="pre-market prep")
        logger.info("Hot window armed for market open (10 min)")
    except Exception:
        logger.exception("Failed to arm hot window for pre-market")


def run_pre_market_loop() -> None:
    """Background loop — fires run_pre_market_prep once on Sunday ~21:00 UTC."""
    logger.info("Pre-market worker started (fires Sunday %02d:00 UTC)", PRE_MARKET_HOUR_UTC)
    time.sleep(120)  # let other workers boot

    last_fired_date = None

    while True:
        now = datetime.now(tz=timezone.utc)
        today = now.date()

        if (
            now.weekday() == PRE_MARKET_WEEKDAY
            and now.hour >= PRE_MARKET_HOUR_UTC
            and last_fired_date != today
        ):
            try:
                run_pre_market_prep()
                last_fired_date = today
            except Exception:
                logger.exception("Pre-market prep crashed")

        time.sleep(60)
