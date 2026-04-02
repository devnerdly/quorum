"""Haiku agent — summarises analysis scores in plain language."""

from __future__ import annotations

import logging

import anthropic

from shared.config import settings

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
FALLBACK = "Unable to generate Haiku summary at this time."


def summarize_scores(scores: dict) -> str:
    """Call claude-haiku to produce a 3-4 sentence outlook summary.

    Parameters
    ----------
    scores:
        Dict containing keys such as technical_score, fundamental_score,
        sentiment_score, shipping_score, unified_score (values may be None).

    Returns
    -------
    str
        A short narrative summary, or a fallback string on error.
    """
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    scores_text = "\n".join(
        f"  {k}: {v}" for k, v in scores.items()
    )
    prompt = (
        "You are a Brent crude oil market analyst assistant. "
        "Below are composite analysis scores (scale: -1.0 = very bearish, +1.0 = very bullish):\n\n"
        f"{scores_text}\n\n"
        "Write a concise 3-4 sentence summary covering the technical, fundamental, "
        "and sentiment outlook for Brent crude oil based on these scores. "
        "Be direct and factual."
    )

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception:
        logger.exception("Haiku summarize_scores failed")
        return FALLBACK
