"""Opus agent — synthesises a final trading recommendation."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

from anthropic import Anthropic

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.signals import AIRecommendation

logger = logging.getLogger(__name__)

MODEL = "claude-opus-4-6"

SYSTEM_PROMPT = (
    "You are a senior oil market strategist with 20+ years of experience trading Brent crude oil. "
    "Your role is to synthesise quantitative scores, qualitative analysis, and social-media sentiment "
    "into a clear, actionable trading recommendation.\n\n"
    "Always respond with a single JSON object (no markdown, no extra text) containing exactly these keys:\n"
    "  unified_score        — float, the synthesised directional score (-1.0 to +1.0)\n"
    "  opus_override_score  — float or null, your score if you disagree with the input unified_score\n"
    "  confidence           — float, your confidence in the recommendation (0.0 to 1.0)\n"
    "  action               — string, one of: BUY, SELL, HOLD, WAIT\n"
    "  analysis_text        — string, 2-4 sentence reasoning for the recommendation\n"
    "  base_scenario        — string, most-likely price outcome over next 24-48 hours\n"
    "  alt_scenario         — string, alternative scenario if key assumptions break\n"
    "  risk_factors         — list of strings, top 3-5 risk factors\n"
    "  entry_price          — float or null, suggested entry price\n"
    "  stop_loss            — float or null, suggested stop-loss level\n"
    "  take_profit          — float or null, suggested take-profit level"
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

    recent = get_recent_signals()
    recent_text = json.dumps(recent, indent=2) if recent else "No recent signals."

    scores_text = json.dumps(scores, indent=2)

    user_prompt = (
        f"## Quantitative Scores\n{scores_text}\n\n"
        f"## Haiku Analyst Summary\n{haiku_summary}\n\n"
        f"## Twitter/X Sentiment (Grok)\n{grok_narrative}\n\n"
        f"## Recent Recommendations (last {len(recent)})\n{recent_text}\n\n"
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
