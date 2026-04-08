"""Opus agent — synthesises a final trading recommendation."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

from anthropic import Anthropic

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.knowledge import KnowledgeSummary
from shared.models.ohlcv import OHLCV
from shared.models.signals import AIRecommendation

logger = logging.getLogger(__name__)

MODEL = "claude-opus-4-6"

SYSTEM_PROMPT = (
    "You are a senior oil market strategist with 20+ years of experience trading Brent crude oil. "
    "Your role is to synthesise quantitative scores, qualitative analysis, and social-media sentiment "
    "into a clear, actionable trading recommendation. You also actively manage existing positions.\n\n"
    "IMPORTANT: All scores in this prompt are on a -100..+100 scale. "
    "0 = neutral, ±50 = strong signal, ±100 = extreme. "
    "Return unified_score on the same -100..+100 scale.\n\n"
    "Always respond with a single JSON object (no markdown, no extra text) containing exactly these keys:\n"
    "  unified_score        — float on -100..+100 scale (0=neutral, ±50=strong signal, ±100=extreme)\n"
    "  opus_override_score  — float or null, your score if you disagree with the input unified_score,\n"
    "                         also on -100..+100 scale\n"
    "  confidence           — float, your confidence in the recommendation (0.0 to 1.0)\n"
    "  action               — string, one of: BUY, SELL, HOLD, WAIT\n"
    "                          - BUY/SELL = open a NEW position now\n"
    "                          - HOLD     = no new entry but existing positions stay open\n"
    "                          - WAIT     = no entry, no new positions, sit on hands\n"
    "  analysis_text        — string, 2-4 sentence reasoning for the recommendation\n"
    "  base_scenario        — string, most-likely price outcome over next 24-48 hours\n"
    "  alt_scenario         — string, alternative scenario if key assumptions break\n"
    "  risk_factors         — list of strings, top 3-5 risk factors\n"
    "  entry_price          — float or null, suggested entry price (REQUIRED for BUY/SELL)\n"
    "  stop_loss            — float or null, suggested stop-loss level\n"
    "  take_profit          — float or null, suggested take-profit level\n"
    "  manage_positions     — list of {id, action, reason} objects, one per OPEN position you want\n"
    "                         to manage. Allowed actions: 'hold' (default), 'close' (exit now).\n"
    "                         If you want to keep all open positions, return an empty list."
)

FALLBACK_REC: dict = {
    "unified_score": None,
    "opus_override_score": None,
    "confidence": 0.0,
    "action": "WAIT",
    "analysis_text": "Opus synthesis failed — recommend waiting for next cycle.",
    "base_scenario": None,
    "alt_scenario": None,
    "risk_factors": [],
    "entry_price": None,
    "stop_loss": None,
    "take_profit": None,
}


def parse_opus_response(text: str) -> dict:
    """Parse the JSON blob returned by Opus.

    Strips optional markdown code fences (```json … ```) before parsing.

    Parameters
    ----------
    text:
        Raw string returned by the model.

    Returns
    -------
    dict
        Parsed recommendation dictionary.

    Raises
    ------
    ValueError
        If no valid JSON object can be extracted.
    """
    # Strip markdown code fences if present
    cleaned = text.strip()
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if fence_match:
        cleaned = fence_match.group(1).strip()

    # Try to extract a JSON object directly
    brace_match = re.search(r"\{[\s\S]*\}", cleaned)
    if brace_match:
        cleaned = brace_match.group(0)

    return json.loads(cleaned)


def get_market_snapshot() -> dict:
    """Return current market snapshot: latest price + recent OHLCV stats.

    Prefers Stooq ICE Brent (matches XTB CFD price) over Yahoo BZ=F (NYMEX
    Brent Last Day Financial, which can drift $0.30-$1.00 from ICE Brent).
    """
    try:
        with SessionLocal() as session:
            # Try Stooq ICE Brent first
            latest = (
                session.query(OHLCV)
                .filter(OHLCV.timeframe == "1min", OHLCV.source == "stooq")
                .order_by(OHLCV.timestamp.desc())
                .first()
            )
            # Fall back to Yahoo if Stooq has no recent data
            if latest is None:
                latest = (
                    session.query(OHLCV)
                    .filter(OHLCV.timeframe == "1min")
                    .order_by(OHLCV.timestamp.desc())
                    .first()
                )
            if latest is None:
                return {}

            # Last 60 1-min bars for short-term range
            recent = (
                session.query(OHLCV)
                .filter(OHLCV.timeframe == "1min")
                .order_by(OHLCV.timestamp.desc())
                .limit(60)
                .all()
            )
            recent_highs = [r.high for r in recent]
            recent_lows = [r.low for r in recent]

            # Last 24 1H bars for medium-term context
            hourly = (
                session.query(OHLCV)
                .filter(OHLCV.timeframe == "1H")
                .order_by(OHLCV.timestamp.desc())
                .limit(24)
                .all()
            )
            hourly_highs = [r.high for r in hourly]
            hourly_lows = [r.low for r in hourly]

            return {
                "current_price": round(latest.close, 2),
                "current_price_source": latest.source,
                "current_timestamp": latest.timestamp.isoformat(),
                "last_60min_high": round(max(recent_highs), 2) if recent_highs else None,
                "last_60min_low": round(min(recent_lows), 2) if recent_lows else None,
                "last_24h_high": round(max(hourly_highs), 2) if hourly_highs else None,
                "last_24h_low": round(min(hourly_lows), 2) if hourly_lows else None,
            }
    except Exception:
        logger.exception("get_market_snapshot failed")
        return {}


def get_recent_knowledge_summaries(limit: int = 6) -> list[dict]:
    """Return the most recent KnowledgeSummary rows (newest first)."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(KnowledgeSummary)
                .order_by(KnowledgeSummary.timestamp.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "timestamp": r.timestamp.isoformat() if r.timestamp else None,
                    "source": r.source,
                    "window": r.window,
                    "msgs": r.message_count,
                    "sentiment_label": r.sentiment_label,
                    "sentiment_score": r.sentiment_score,
                    "summary": r.summary,
                    "key_events": r.key_events,
                }
                for r in rows
            ]
    except Exception:
        logger.exception("get_recent_knowledge_summaries failed")
        return []


def get_recent_signals(limit: int = 5) -> list:
    """Return the most recent AIRecommendation rows from the database.

    Parameters
    ----------
    limit:
        Maximum number of rows to return.

    Returns
    -------
    list[dict]
        List of recommendation dicts (most recent first).
    """
    try:
        with SessionLocal() as session:
            rows = (
                session.query(AIRecommendation)
                .order_by(AIRecommendation.timestamp.desc())
                .limit(limit)
                .all()
            )
            result = []
            for row in rows:
                result.append(
                    {
                        "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                        "action": row.action,
                        "unified_score": row.unified_score,
                        "opus_override_score": row.opus_override_score,
                        "confidence": row.confidence,
                    }
                )
            return result
    except Exception:
        logger.exception("get_recent_signals failed")
        return []


def synthesize_recommendation(
    scores: dict,
    haiku_summary: str,
    grok_narrative: str,
    open_positions: list[dict] | None = None,
) -> dict:
    """Call Claude Opus to synthesise a trading recommendation.

    Parameters
    ----------
    scores:
        ScoresEvent dict with technical/fundamental/sentiment/unified scores.
    haiku_summary:
        Short outlook summary from the Haiku agent.
    grok_narrative:
        Twitter/X narrative from the Grok agent.

    Returns
    -------
    dict
        Recommendation dictionary ready to be published and stored.
    """
    client = Anthropic(api_key=settings.anthropic_api_key)

    market = get_market_snapshot()
    recent = get_recent_signals()
    if recent:
        actions = [r.get("action", "?") for r in recent]
        confs = [r.get("confidence") or 0 for r in recent]
        avg_conf = sum(confs) / len(confs) if confs else 0
        action_counts = {a: actions.count(a) for a in set(actions)}
        summary = ", ".join(f"{a}×{n}" for a, n in action_counts.items())
        recent_text = f"Last {len(recent)} actions: {summary} | avg confidence {avg_conf:.2f}"
    else:
        recent_text = "No recent signals."

    knowledge = get_recent_knowledge_summaries()
    if knowledge:
        knowledge_text = json.dumps(knowledge, indent=2, default=str)
    else:
        knowledge_text = "No knowledge summaries yet."

    scores_text = json.dumps(scores, indent=2)
    market_text = json.dumps(market, indent=2) if market else "No market data."

    if open_positions:
        positions_text = json.dumps(open_positions, indent=2, default=str)
        positions_block = (
            f"## Currently Open Positions ({len(open_positions)})\n"
            f"{positions_text}\n\n"
            "For each open position, decide whether to HOLD or CLOSE based on the "
            "current market state and unrealised P/L. Return your decisions in "
            "manage_positions. If you choose to add a NEW position on top, set "
            "action=BUY/SELL with entry_price; otherwise set action=HOLD or WAIT.\n\n"
        )
    else:
        positions_block = (
            "## Currently Open Positions\nNone — feel free to open a new position if "
            "the setup is strong, otherwise WAIT.\n\n"
        )

    user_prompt = (
        f"## Current Brent Crude Market Snapshot (USE THESE EXACT PRICES)\n{market_text}\n\n"
        f"{positions_block}"
        f"## Knowledge Base — Recent @marketfeed Digests (newest first)\n{knowledge_text}\n\n"
        f"## Quantitative Scores\n{scores_text}\n\n"
        f"## Haiku Analyst Summary\n{haiku_summary}\n\n"
        f"## Twitter/X Sentiment (Grok)\n{grok_narrative}\n\n"
        f"## Recent Recommendations (last {len(recent)})\n{recent_text}\n\n"
        "CRITICAL: entry_price, stop_loss, and take_profit MUST be derived from the "
        "current_price above (within ±5% for entry, realistic SL/TP for the timeframe). "
        "Do NOT invent prices from training data. If you cannot anchor to current_price, "
        "set them to null.\n\n"
        "PRIORITISE the @marketfeed knowledge base — these are the most recent breaking-news "
        "events that move the oil market. Reference them in your analysis_text.\n\n"
        "Based on all of the above, provide your trading recommendation as a JSON object."
    )

    timestamp = datetime.now(timezone.utc)

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=800,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw_text = response.content[0].text
        rec = parse_opus_response(raw_text)
    except Exception:
        logger.exception("Opus synthesize_recommendation failed")
        rec = dict(FALLBACK_REC)

    # Attach context fields
    rec["timestamp"] = timestamp.isoformat()
    rec["haiku_summary"] = haiku_summary
    rec["grok_narrative"] = grok_narrative

    # Ensure required fields have defaults
    rec.setdefault("unified_score", scores.get("unified_score"))

    # Persist to database
    try:
        _store_recommendation(rec, timestamp)
    except Exception:
        logger.exception("Failed to persist AIRecommendation to DB")

    return rec


def _store_recommendation(rec: dict, timestamp: datetime) -> None:
    """Persist an AIRecommendation record to the database.

    Parameters
    ----------
    rec:
        Recommendation dictionary as returned by Opus (or the fallback).
    timestamp:
        UTC datetime for the record.
    """
    risk_factors = rec.get("risk_factors")
    if isinstance(risk_factors, list):
        risk_factors = json.dumps(risk_factors)

    row = AIRecommendation(
        timestamp=timestamp,
        unified_score=rec.get("unified_score"),
        opus_override_score=rec.get("opus_override_score"),
        confidence=rec.get("confidence"),
        action=rec.get("action", "WAIT"),
        analysis_text=rec.get("analysis_text"),
        base_scenario=rec.get("base_scenario"),
        alt_scenario=rec.get("alt_scenario"),
        risk_factors=risk_factors,
        entry_price=rec.get("entry_price"),
        stop_loss=rec.get("stop_loss"),
        take_profit=rec.get("take_profit"),
        haiku_summary=rec.get("haiku_summary"),
        grok_narrative=rec.get("grok_narrative"),
    )

    with SessionLocal() as session:
        session.add(row)
        session.commit()
