"""Grok agent — retrieves Twitter/X crude oil sentiment narrative."""

from __future__ import annotations

import logging

from openai import OpenAI

from shared.config import settings

logger = logging.getLogger(__name__)

MODEL = "grok-3"
FALLBACK = "Unable to retrieve Grok narrative at this time."


def get_twitter_narrative() -> str:
    """Call Grok to describe the current Twitter/X crude oil sentiment.

    Returns
    -------
    str
        A short narrative about social media sentiment, or a fallback string
        on error.
    """
    client = OpenAI(
        api_key=settings.xai_api_key,
        base_url="https://api.x.ai/v1",
    )

    prompt = (
        "You have real-time access to Twitter/X. "
        "Describe the current Twitter/X narrative and sentiment around Brent crude oil prices. "
        "What are traders, analysts, and news accounts saying right now? "
        "Summarise the dominant themes in 2-3 sentences. Be concise and factual."
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
