"""Campaign management plugin — Wave 5B.

Defines PLUGIN_TOOLS (list of Anthropic tool schemas) and an execute() dispatcher.
Loaded by chat.py alongside chat_tools.TOOLS so Opus can call these tools in the
same agentic loop.

Tools:
    partial_close_campaign   — close a % of open layers (oldest-first)
    update_campaign_limits   — change hard-stop max_loss_pct on a live campaign
    update_campaign_tp       — set / change the campaign-level take-profit price
    review_closed_campaign   — post-mortem data dump for a closed campaign
    add_campaign_note        — append a timestamped note to a campaign
    get_performance_summary  — stats over the last N days of closed campaigns
    remember_fact            — persist a user belief / macro thesis
    recall_facts             — retrieve stored facts (topic / keyword filter)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

PLUGIN_TOOLS: list[dict] = [
    {
        "name": "partial_close_campaign",
        "description": (
            "Close a percentage of a campaign's open layers at the current market price. "
            "Useful for taking partial profit. Closes layers starting from the oldest until "
            "the requested fraction of total lots is closed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "integer"},
                "pct": {
                    "type": "number",
                    "description": "Percentage of total lots to close, 0-100",
                },
                "reason": {"type": "string"},
            },
            "required": ["campaign_id", "pct", "reason"],
        },
    },
    {
        "name": "update_campaign_limits",
        "description": (
            "Update the maximum drawdown limit (hard stop %) for an open campaign. "
            "Use to tighten or loosen the auto-close threshold."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "integer"},
                "max_loss_pct": {
                    "type": "number",
                    "description": "New max loss %, e.g. 30 for tighter, 60 for looser",
                },
            },
            "required": ["campaign_id", "max_loss_pct"],
        },
    },
    {
        "name": "update_campaign_tp",
        "description": (
            "Set or update a take-profit price for a campaign. "
            "When Brent reaches this level, the bot will auto-close the entire campaign."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "integer"},
                "take_profit_price": {"type": "number"},
            },
            "required": ["campaign_id", "take_profit_price"],
        },
    },
    {
        "name": "review_closed_campaign",
        "description": (
            "Produce a post-mortem analysis of a closed campaign: entry timing, "
            "DCA layer effectiveness, final PnL, what the market was doing, recommended lessons. "
            "Opus should pull campaign detail + price history around the campaign's lifetime "
            "and produce a narrative."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "integer"},
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "add_campaign_note",
        "description": (
            "Append a free-text note to a campaign. "
            "Useful for recording thesis, news context, or manual observations."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_id": {"type": "integer"},
                "note": {"type": "string"},
            },
            "required": ["campaign_id", "note"],
        },
    },
    {
        "name": "get_performance_summary",
        "description": (
            "Compute performance stats over the last N days: number of campaigns, "
            "win rate, avg PnL, biggest win/loss, total realized PnL, current equity delta."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 30},
            },
        },
    },
    {
        "name": "remember_fact",
        "description": (
            "Store a user-provided fact or belief for later recall. "
            "Examples: 'I believe OPEC will cut production in September', "
            "'I'm bullish on Brent above $95'. Returns the stored fact id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "fact": {"type": "string"},
                "topic": {
                    "type": "string",
                    "description": "Optional topic/tag for grouping",
                },
            },
            "required": ["fact"],
        },
    },
    {
        "name": "recall_facts",
        "description": (
            "Retrieve previously stored user facts, "
            "optionally filtered by topic or keyword substring."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "keyword": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def execute(name: str, tool_input: dict) -> dict | None:
    """Route plugin tool calls.  Returns None if the tool name is not ours."""
    if name == "partial_close_campaign":
        return _partial_close_campaign(**tool_input)
    if name == "update_campaign_limits":
        return _update_campaign_limits(**tool_input)
    if name == "update_campaign_tp":
        return _update_campaign_tp(**tool_input)
    if name == "review_closed_campaign":
        return _review_closed_campaign(**tool_input)
    if name == "add_campaign_note":
        return _add_campaign_note(**tool_input)
    if name == "get_performance_summary":
        return _get_performance_summary(**tool_input)
    if name == "remember_fact":
        return _remember_fact(**tool_input)
    if name == "recall_facts":
        return _recall_facts(**tool_input)
    return None


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------

def _partial_close_campaign(campaign_id: int, pct: float, reason: str) -> dict:
    from shared.position_manager import get_current_price, partial_close_campaign
    from shared.redis_streams import publish

    price = get_current_price()
    if price is None:
        return {"error": "no current price available"}

    result = partial_close_campaign(campaign_id, pct, price, reason)
    if "error" in result:
        return result

    try:
        publish(
            "position.event",
            {
                "type": "partial_close",
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                **result,
            },
        )
    except Exception:
        pass

    return result


def _update_campaign_limits(campaign_id: int, max_loss_pct: float) -> dict:
    from shared.position_manager import update_campaign_limits

    return update_campaign_limits(campaign_id, max_loss_pct)


def _update_campaign_tp(campaign_id: int, take_profit_price: float) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.campaigns import Campaign

    with SessionLocal() as session:
        campaign = session.query(Campaign).filter(Campaign.id == campaign_id).first()
        if campaign is None:
            return {"error": f"campaign {campaign_id} not found"}
        if campaign.status != "open":
            return {"error": f"campaign {campaign_id} is not open (status={campaign.status})"}

        old_tp = campaign.take_profit
        campaign.take_profit = take_profit_price
        session.commit()

    logger.info(
        "update_campaign_tp #%s: take_profit %s → %.2f",
        campaign_id,
        f"{old_tp:.2f}" if old_tp is not None else "None",
        take_profit_price,
    )
    return {
        "campaign_id": campaign_id,
        "old_take_profit": old_tp,
        "new_take_profit": take_profit_price,
        "updated": True,
    }


def _review_closed_campaign(campaign_id: int) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.campaigns import Campaign
    from shared.models.ohlcv import OHLCV
    from shared.models.positions import Position

    with SessionLocal() as session:
        campaign = session.query(Campaign).filter(Campaign.id == campaign_id).first()
        if campaign is None:
            return {"error": f"campaign {campaign_id} not found"}

        positions = (
            session.query(Position)
            .filter(Position.campaign_id == campaign_id)
            .order_by(Position.opened_at)
            .all()
        )

        opened_at = campaign.opened_at
        closed_at = campaign.closed_at or datetime.now(tz=timezone.utc)

        # Fetch 1H OHLCV bars covering the campaign lifetime
        ohlcv_rows = (
            session.query(OHLCV)
            .filter(
                OHLCV.timeframe == "1H",
                OHLCV.timestamp >= opened_at,
                OHLCV.timestamp <= closed_at,
            )
            .order_by(OHLCV.timestamp)
            .all()
        )

        # Compute duration
        duration_hours = round((closed_at - opened_at).total_seconds() / 3600, 2)

        # Price at open / close from positions
        price_at_open: float | None = positions[0].entry_price if positions else None
        price_at_close: float | None = None
        for p in reversed(positions):
            if p.close_price is not None:
                price_at_close = p.close_price
                break

        # Range during campaign
        highs = [r.high for r in ohlcv_rows if r.high is not None]
        lows = [r.low for r in ohlcv_rows if r.low is not None]
        high_during = max(highs) if highs else None
        low_during = min(lows) if lows else None

        # Favorable / adverse excursion from avg entry
        avg_entry: float | None = None
        total_lots = sum(p.lots or 0.0 for p in positions if p.status != "open")
        total_lots_open = sum(p.lots or 0.0 for p in positions)
        if total_lots_open > 0:
            avg_entry = sum((p.lots or 0.0) * p.entry_price for p in positions) / total_lots_open

        max_favorable_excursion: float | None = None
        max_adverse_excursion: float | None = None
        if avg_entry is not None and high_during is not None and low_during is not None:
            if campaign.side == "LONG":
                max_favorable_excursion = round(high_during - avg_entry, 4)
                max_adverse_excursion = round(avg_entry - low_during, 4)
            else:
                max_favorable_excursion = round(avg_entry - low_during, 4)
                max_adverse_excursion = round(high_during - avg_entry, 4)

        # PnL
        final_pnl = campaign.realized_pnl
        total_margin = sum(p.margin_used or 0.0 for p in positions)
        pnl_pct_of_margin = (
            round((final_pnl / total_margin) * 100, 2)
            if final_pnl is not None and total_margin > 0
            else None
        )

        layers = [
            {
                "position_id": p.id,
                "layer_index": p.layer_index,
                "side": p.side,
                "entry_price": p.entry_price,
                "close_price": p.close_price,
                "lots": p.lots,
                "margin_used": p.margin_used,
                "opened_at": p.opened_at.isoformat() if p.opened_at else None,
                "closed_at": p.closed_at.isoformat() if p.closed_at else None,
                "realised_pnl": p.realised_pnl,
                "status": p.status,
                "notes": p.notes,
            }
            for p in positions
        ]

    return {
        "campaign": {
            "id": campaign_id,
            "side": campaign.side,
            "status": campaign.status,
            "opened_at": campaign.opened_at.isoformat() if campaign.opened_at else None,
            "closed_at": campaign.closed_at.isoformat() if campaign.closed_at else None,
            "max_loss_pct": campaign.max_loss_pct,
            "notes": campaign.notes,
        },
        "layers": layers,
        "layer_count": len(layers),
        "price_at_open": price_at_open,
        "price_at_close": price_at_close,
        "avg_entry_price": round(avg_entry, 5) if avg_entry is not None else None,
        "high_during": high_during,
        "low_during": low_during,
        "max_favorable_excursion": max_favorable_excursion,
        "max_adverse_excursion": max_adverse_excursion,
        "duration_hours": duration_hours,
        "total_margin": round(total_margin, 2),
        "final_pnl": round(final_pnl, 2) if final_pnl is not None else None,
        "pnl_pct_of_margin": pnl_pct_of_margin,
        "ohlcv_bar_count": len(ohlcv_rows),
        "note": "Raw data — Opus will produce the post-mortem narrative from this",
    }


def _add_campaign_note(campaign_id: int, note: str) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.campaigns import Campaign

    timestamp_prefix = datetime.now(tz=timezone.utc).strftime("[%Y-%m-%d %H:%M]")
    stamped_note = f"{timestamp_prefix} {note}\n"

    with SessionLocal() as session:
        campaign = session.query(Campaign).filter(Campaign.id == campaign_id).first()
        if campaign is None:
            return {"error": f"campaign {campaign_id} not found"}

        campaign.notes = (campaign.notes + stamped_note) if campaign.notes else stamped_note
        session.commit()

    logger.info("add_campaign_note #%s: appended note", campaign_id)
    return {
        "campaign_id": campaign_id,
        "appended": stamped_note,
        "success": True,
    }


def _get_performance_summary(days: int = 30) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.campaigns import Campaign
    from shared.account_manager import recompute_account_state

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)

    with SessionLocal() as session:
        closed_camps = (
            session.query(Campaign)
            .filter(
                Campaign.status != "open",
                Campaign.closed_at >= cutoff,
            )
            .order_by(Campaign.closed_at)
            .all()
        )

        pnls = [c.realized_pnl for c in closed_camps if c.realized_pnl is not None]
        count = len(closed_camps)
        won_count = sum(1 for p in pnls if p > 0)
        lost_count = sum(1 for p in pnls if p <= 0)
        win_rate = round(won_count / count * 100, 1) if count > 0 else None
        avg_pnl = round(sum(pnls) / len(pnls), 2) if pnls else None
        biggest_win = round(max(pnls), 2) if pnls else None
        biggest_loss = round(min(pnls), 2) if pnls else None
        total_realized_pnl = round(sum(pnls), 2)

    # Account equity delta
    try:
        account_state = recompute_account_state()
        equity_delta = round(
            account_state["equity"] - account_state["starting_balance"], 2
        )
    except Exception:
        account_state = None
        equity_delta = None

    return {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "count": count,
        "won_count": won_count,
        "lost_count": lost_count,
        "win_rate_pct": win_rate,
        "avg_pnl": avg_pnl,
        "biggest_win": biggest_win,
        "biggest_loss": biggest_loss,
        "total_realized_pnl": total_realized_pnl,
        "equity_delta": equity_delta,
        "account": account_state,
    }


def _remember_fact(fact: str, topic: str | None = None) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.facts import Fact

    now = datetime.now(tz=timezone.utc)

    with SessionLocal() as session:
        row = Fact(
            created_at=now,
            topic=topic,
            content=fact,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        fact_id = row.id

    logger.info("remember_fact #%s (topic=%s): %s", fact_id, topic, fact[:80])
    return {
        "fact_id": fact_id,
        "created_at": now.isoformat(),
        "topic": topic,
        "content": fact,
        "stored": True,
    }


def _recall_facts(
    topic: str | None = None,
    keyword: str | None = None,
    limit: int = 20,
) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.facts import Fact

    with SessionLocal() as session:
        query = session.query(Fact).order_by(Fact.created_at.desc())
        if topic:
            query = query.filter(Fact.topic == topic)
        if keyword:
            query = query.filter(Fact.content.ilike(f"%{keyword}%"))
        rows = query.limit(limit).all()

        facts = [
            {
                "id": r.id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "topic": r.topic,
                "content": r.content,
            }
            for r in rows
        ]

    return {
        "count": len(facts),
        "topic_filter": topic,
        "keyword_filter": keyword,
        "facts": facts,
    }
