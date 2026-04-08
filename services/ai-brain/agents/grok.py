"""Grok agent — retrieves Twitter/X crude oil sentiment narrative."""

from __future__ import annotations

import logging

from openai import OpenAI
from sqlalchemy import desc

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)

MODEL = "grok-3"
FALLBACK = "Unable to retrieve Grok narrative at this time."


def _get_current_price() -> float | None:
    """Return the most recent Brent close (prefers Stooq ICE Brent)."""
    try:
        with SessionLocal() as session:
            row = (
                session.query(OHLCV)
                .filter(OHLCV.timeframe == "1min", OHLCV.source == "stooq")
                .order_by(desc(OHLCV.timestamp))
                .first()
            )
            if row is None:
                row = (
                    session.query(OHLCV)
                    .filter(OHLCV.timeframe == "1min")
                    .order_by(desc(OHLCV.timestamp))
                    .first()
                )
            return float(row.close) if row else None
    except Exception:
        logger.exception("Failed to read current price for Grok prompt")
        return None


def get_twitter_narrative() -> str:
    """Call Grok to describe the current Twitter/X crude oil sentiment."""
    client = OpenAI(
        api_key=settings.xai_api_key,
        base_url="https://api.x.ai/v1",
    )

    current_price = _get_current_price()
    price_anchor = (
        f"FACT — current Brent (ICE) price is ${current_price:.2f}. "
        f"Do NOT cite any other price level. Do not invent prices from your training data.\n\n"
        if current_price is not None
        else ""
    )

    prompt = (
        f"{price_anchor}"
        "You have real-time access to Twitter/X. "
        "Describe the current Twitter/X narrative and sentiment around Brent crude oil. "
        "What are traders, analysts, and news accounts saying right now about supply, "
        "demand, geopolitics, and OPEC? "
        "Summarise the dominant themes in 2-3 sentences. Be concise and factual. "
        "If you reference price levels, use only the FACT above."
    )

    try:
        response = client.chat.completions.create(
            model=MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content.strip()
    except Exception:
        logger.exception("Grok get_twitter_narrative failed")
        return FALLBACK
