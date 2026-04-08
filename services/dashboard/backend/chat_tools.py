"""Chat tools definitions and dispatcher for the trading assistant."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anthropic tool schemas
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "get_current_market_state",
        "description": (
            "Get the current Brent crude market state: latest price (Stooq ICE preferred), "
            "all 5 sub-scores, top 3 knowledge digests, list of open positions, and the latest "
            "AIRecommendation. Always call this first before answering any 'should I...' question."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_price_history",
        "description": "Get OHLCV price history for a given timeframe.",
        "input_schema": {
            "type": "object",
            "properties": {
                "timeframe": {
                    "type": "string",
                    "enum": ["1min", "5min", "15min", "1H", "1D", "1W"],
                },
                "limit": {"type": "integer", "default": 100},
            },
            "required": ["timeframe"],
        },
    },
    {
        "name": "query_marketfeed",
        "description": (
            "Search recent @marketfeed knowledge digests for breaking news. "
            "Returns digests within last N hours, optionally filtered by keyword."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "default": 6},
                "keyword": {"type": "string"},
            },
        },
    },
    {
        "name": "get_signal_detail",
        "description": (
            "Get the full analysis text and trade levels of a specific past AI recommendation by id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"signal_id": {"type": "integer"}},
            "required": ["signal_id"],
        },
    },
    {
        "name": "get_recent_signals",
        "description": "List the most recent AI trading signals (BUY/SELL/HOLD/WAIT recommendations).",
        "input_schema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "default": 10}},
        },
    },
    {
        "name": "get_open_positions",
        "description": "List all currently open trading positions with live unrealised PnL.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "simulate_trade",
        "description": (
            "Simulate the risk/reward of a hypothetical trade. Returns R:R ratio, % risk, "
            "distance to SL/TP in price and percent."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "side": {"type": "string", "enum": ["LONG", "SHORT"]},
                "entry": {"type": "number"},
                "stop_loss": {"type": "number"},
                "take_profit": {"type": "number"},
            },
            "required": ["side", "entry", "stop_loss", "take_profit"],
        },
    },
]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def execute_tool(name: str, tool_input: dict) -> dict:
    """Dispatch a tool call to its implementation. Returns serialisable dict."""
    if name == "get_current_market_state":
        return _get_current_market_state()
    if name == "get_price_history":
        return _get_price_history(**tool_input)
    if name == "query_marketfeed":
        return _query_marketfeed(**tool_input)
    if name == "get_signal_detail":
        return _get_signal_detail(**tool_input)
    if name == "get_recent_signals":
        return _get_recent_signals(**tool_input)
    if name == "get_open_positions":
        return _get_open_positions()
    if name == "simulate_trade":
        return _simulate_trade(**tool_input)
    return {"error": f"unknown tool: {name}"}


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def _get_current_market_state() -> dict:
    from shared.models.base import SessionLocal
    from shared.models.ohlcv import OHLCV
    from shared.models.signals import AnalysisScore, AIRecommendation
    from shared.models.knowledge import KnowledgeSummary
    from shared.position_manager import list_open_positions
    from sqlalchemy import desc

    with SessionLocal() as session:
        # Latest Stooq price (preferred), fallback to any 1-min bar
        price_row = (
            session.query(OHLCV)
            .filter(OHLCV.timeframe == "1min", OHLCV.source == "stooq")
            .order_by(desc(OHLCV.timestamp))
            .first()
        )
        if price_row is None:
            price_row = (
                session.query(OHLCV)
                .filter(OHLCV.timeframe == "1min")
                .order_by(desc(OHLCV.timestamp))
                .first()
            )

        # Latest composite scores
        scores = (
            session.query(AnalysisScore)
            .order_by(desc(AnalysisScore.timestamp))
            .first()
        )

        # Latest AI recommendation
        rec = (
            session.query(AIRecommendation)
            .order_by(desc(AIRecommendation.timestamp))
            .first()
        )

        # Top 3 most recent knowledge digests
        knowledge = (
            session.query(KnowledgeSummary)
            .order_by(desc(KnowledgeSummary.timestamp))
            .limit(3)
            .all()
        )

    return {
        "current_price": round(price_row.close, 2) if price_row else None,
        "current_price_source": price_row.source if price_row else None,
        "current_price_timestamp": price_row.timestamp.isoformat() if price_row else None,
        "scores": {
            "technical": scores.technical_score if scores else None,
            "fundamental": scores.fundamental_score if scores else None,
            "sentiment": scores.sentiment_score if scores else None,
            "shipping": scores.shipping_score if scores else None,
            "unified": scores.unified_score if scores else None,
        } if scores else None,
        "open_positions": list_open_positions(),
        "latest_recommendation": {
            "id": rec.id,
            "timestamp": rec.timestamp.isoformat(),
            "action": rec.action,
            "confidence": rec.confidence,
            "analysis_text": rec.analysis_text,
        } if rec else None,
        "recent_knowledge": [
            {
                "timestamp": k.timestamp.isoformat(),
                "summary": k.summary,
                "sentiment_label": k.sentiment_label,
                "sentiment_score": k.sentiment_score,
            }
            for k in knowledge
        ],
    }


def _get_price_history(timeframe: str, limit: int = 100) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.ohlcv import OHLCV
    from sqlalchemy import desc

    with SessionLocal() as session:
        rows = (
            session.query(OHLCV)
            .filter(OHLCV.timeframe == timeframe)
            .order_by(desc(OHLCV.timestamp))
            .limit(limit)
            .all()
        )
        # Return in ascending time order
        rows = list(reversed(rows))
        return {
            "timeframe": timeframe,
            "count": len(rows),
            "bars": [
                {
                    "timestamp": r.timestamp.isoformat(),
                    "open": r.open,
                    "high": r.high,
                    "low": r.low,
                    "close": r.close,
                    "volume": r.volume,
                    "source": r.source,
                }
                for r in rows
            ],
        }


def _query_marketfeed(hours: int = 6, keyword: str | None = None) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.knowledge import KnowledgeSummary
    from sqlalchemy import desc

    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)

    with SessionLocal() as session:
        query = (
            session.query(KnowledgeSummary)
            .filter(KnowledgeSummary.timestamp >= cutoff)
        )
        if keyword:
            query = query.filter(KnowledgeSummary.summary.ilike(f"%{keyword}%"))
        rows = query.order_by(desc(KnowledgeSummary.timestamp)).limit(50).all()

        digests = []
        for k in rows:
            key_events = []
            if k.key_events:
                try:
                    key_events = json.loads(k.key_events)
                except Exception:
                    key_events = []
            digests.append(
                {
                    "id": k.id,
                    "timestamp": k.timestamp.isoformat(),
                    "source": k.source,
                    "window": k.window,
                    "message_count": k.message_count,
                    "summary": k.summary,
                    "key_events": key_events,
                    "sentiment_score": k.sentiment_score,
                    "sentiment_label": k.sentiment_label,
                }
            )

    return {
        "hours": hours,
        "keyword": keyword,
        "count": len(digests),
        "digests": digests,
    }


def _get_signal_detail(signal_id: int) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.signals import AIRecommendation, AnalysisScore
    from shared.models.knowledge import KnowledgeSummary
    from sqlalchemy import desc

    with SessionLocal() as session:
        rec = (
            session.query(AIRecommendation)
            .filter(AIRecommendation.id == signal_id)
            .first()
        )
        if rec is None:
            return {"error": f"signal {signal_id} not found"}

        window_start = rec.timestamp - timedelta(minutes=15)
        window_end = rec.timestamp + timedelta(minutes=15)

        scores = (
            session.query(AnalysisScore)
            .filter(AnalysisScore.timestamp.between(window_start, window_end))
            .order_by(desc(AnalysisScore.timestamp))
            .first()
        )

        nearby_knowledge = (
            session.query(KnowledgeSummary)
            .filter(KnowledgeSummary.timestamp.between(window_start, window_end))
            .order_by(desc(KnowledgeSummary.timestamp))
            .all()
        )

        risk_factors = []
        if rec.risk_factors:
            try:
                risk_factors = json.loads(rec.risk_factors)
            except Exception:
                risk_factors = []

        return {
            "id": rec.id,
            "timestamp": rec.timestamp.isoformat(),
            "action": rec.action,
            "confidence": rec.confidence,
            "unified_score": rec.unified_score,
            "opus_override_score": rec.opus_override_score,
            "analysis_text": rec.analysis_text,
            "base_scenario": rec.base_scenario,
            "alt_scenario": rec.alt_scenario,
            "risk_factors": risk_factors,
            "entry_price": rec.entry_price,
            "stop_loss": rec.stop_loss,
            "take_profit": rec.take_profit,
            "haiku_summary": rec.haiku_summary,
            "grok_narrative": rec.grok_narrative,
            "scores_at_signal": {
                "technical_score": scores.technical_score,
                "fundamental_score": scores.fundamental_score,
                "sentiment_score": scores.sentiment_score,
                "shipping_score": scores.shipping_score,
                "unified_score": scores.unified_score,
            } if scores else None,
            "knowledge_summaries_nearby": [
                {
                    "id": k.id,
                    "timestamp": k.timestamp.isoformat(),
                    "summary": k.summary,
                    "key_events": json.loads(k.key_events) if k.key_events else [],
                    "sentiment_score": k.sentiment_score,
                    "sentiment_label": k.sentiment_label,
                }
                for k in nearby_knowledge
            ],
        }


def _get_recent_signals(limit: int = 10) -> dict:
    from shared.models.base import SessionLocal
    from shared.models.signals import AIRecommendation
    from sqlalchemy import desc

    with SessionLocal() as session:
        rows = (
            session.query(AIRecommendation)
            .order_by(desc(AIRecommendation.timestamp))
            .limit(limit)
            .all()
        )
        return {
            "count": len(rows),
            "signals": [
                {
                    "id": r.id,
                    "timestamp": r.timestamp.isoformat(),
                    "action": r.action,
                    "confidence": r.confidence,
                    "unified_score": r.unified_score,
                    "entry_price": r.entry_price,
                    "stop_loss": r.stop_loss,
                    "take_profit": r.take_profit,
                    "haiku_summary": r.haiku_summary,
                }
                for r in rows
            ],
        }


def _get_open_positions() -> dict:
    from shared.position_manager import list_open_positions

    positions = list_open_positions()
    return {
        "count": len(positions),
        "positions": positions,
    }


def _simulate_trade(
    side: str,
    entry: float,
    stop_loss: float,
    take_profit: float,
) -> dict:
    """Compute basic R:R metrics for a hypothetical trade."""
    side = side.upper()
    if side == "LONG":
        risk = entry - stop_loss
        reward = take_profit - entry
    elif side == "SHORT":
        risk = stop_loss - entry
        reward = entry - take_profit
    else:
        return {"error": f"invalid side: {side}"}

    if risk <= 0:
        return {"error": "stop_loss is on the wrong side of entry for this trade direction"}
    if reward <= 0:
        return {"error": "take_profit is on the wrong side of entry for this trade direction"}

    rr_ratio = round(reward / risk, 2)
    risk_pct = round((risk / entry) * 100, 3)
    reward_pct = round((reward / entry) * 100, 3)
    sl_distance = round(abs(entry - stop_loss), 4)
    tp_distance = round(abs(take_profit - entry), 4)

    return {
        "side": side,
        "entry": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "risk_points": round(risk, 4),
        "reward_points": round(reward, 4),
        "rr_ratio": rr_ratio,
        "risk_pct": risk_pct,
        "reward_pct": reward_pct,
        "sl_distance": sl_distance,
        "tp_distance": tp_distance,
        "verdict": (
            "Good R:R" if rr_ratio >= 2.0
            else "Marginal R:R" if rr_ratio >= 1.5
            else "Poor R:R"
        ),
    }
