"""RSS news collector with Claude Haiku sentiment classification."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

import anthropic
import feedparser
import requests

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.sentiment import SentimentNews
from shared.redis_streams import publish
from shared.schemas.events import SentimentEvent

logger = logging.getLogger(__name__)

FEEDS: list[dict[str, str]] = [
    {
        "name": "oilprice_main",
        "url": "https://oilprice.com/rss/main",
    },
    {
        "name": "oilprice_geopolitics",
        "url": "https://oilprice.com/rss/geopolitics",
    },
    {
        "name": "oilprice_breaking",
        "url": "https://oilprice.com/rss/breaking",
    },
    {
        "name": "rigzone",
        "url": "https://www.rigzone.com/news/rss/rigzone_latest.aspx",
    },
]

_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BrentBot/1.0)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

_STREAM = "sentiment.news"
_HAIKU_MODEL = "claude-haiku-4-5-20251001"

_CLASSIFY_SYSTEM = (
    "You are a financial news classifier specialised in crude oil markets. "
    "Respond only with a JSON object and no other text."
)

_CLASSIFY_TEMPLATE = """Classify the following news headline for Brent crude oil market sentiment.

Title: {title}
Source: {source}

Return a JSON object with exactly these keys:
- sentiment: one of "bullish", "bearish", or "neutral"
- score: float between -1.0 (very bearish) and 1.0 (very bullish)
- relevance: float between 0.0 and 1.0 indicating how relevant this headline is to Brent crude oil prices

Example: {{"sentiment": "bullish", "score": 0.6, "relevance": 0.9}}"""


def classify_article(title: str, source: str) -> dict:
    """Call Claude Haiku to classify a news article headline.

    Returns a dict with keys: sentiment, score, relevance.
    Falls back to neutral/0/0 on any error.
    """
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    prompt = _CLASSIFY_TEMPLATE.format(title=title, source=source)

    try:
        message = client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=128,
            system=_CLASSIFY_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

        # Strip optional ```json ... ``` markdown fences
        fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
        if fence:
            raw = fence.group(1).strip()

        # Fall back to grabbing the first {...} object if there is surrounding text
        if not raw.startswith("{"):
            brace = re.search(r"\{[\s\S]*\}", raw)
            if brace:
                raw = brace.group(0)

        data = json.loads(raw)
        return {
            "sentiment": str(data.get("sentiment", "neutral")),
            "score": float(data.get("score", 0.0)),
            "relevance": float(data.get("relevance", 0.0)),
        }
    except Exception:
        logger.exception(
            "Haiku classification failed for title=%r — raw response: %r",
            title,
            locals().get("raw", ""),
        )
        return {"sentiment": "neutral", "score": 0.0, "relevance": 0.0}


def fetch_and_classify() -> list[dict]:
    """Parse all RSS feeds and classify each entry with Haiku.

    Returns a list of dicts with keys:
        title, url, source, sentiment, score, relevance, published_at
    """
    results: list[dict] = []

    for feed_cfg in FEEDS:
        feed_name = feed_cfg["name"]
        try:
            response = requests.get(feed_cfg["url"], headers=_HTTP_HEADERS, timeout=20)
            response.raise_for_status()
            feed = feedparser.parse(response.content)
        except Exception as exc:
            logger.warning("Failed to fetch RSS feed %s: %s", feed_name, exc)
            continue

        if feed.bozo and not feed.entries:
            logger.warning("Feed %s parse error: %s", feed_name, feed.bozo_exception)
            continue

        for entry in feed.entries:
            title = entry.get("title", "").strip()
            url = entry.get("link", "")

            if not title:
                continue

            # Parse published date; fall back to now
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                import time
                published_at = datetime.fromtimestamp(
                    time.mktime(entry.published_parsed), tz=timezone.utc
                )
            else:
                published_at = datetime.now(tz=timezone.utc)

            classification = classify_article(title, feed_name)
            results.append(
                {
                    "title": title,
                    "url": url,
                    "source": feed_name,
                    "sentiment": classification["sentiment"],
                    "score": classification["score"],
                    "relevance": classification["relevance"],
                    "published_at": published_at,
                }
            )

    logger.info("Fetched and classified %d articles from RSS feeds", len(results))
    return results


def collect_and_store() -> None:
    """Fetch, classify, filter, persist, and publish RSS sentiment."""
    articles = fetch_and_classify()
    if not articles:
        logger.info("No RSS articles to store")
        return

    # Filter by relevance threshold
    relevant = [a for a in articles if a["relevance"] >= 0.3]
    logger.info("%d/%d articles pass relevance>=0.3 filter", len(relevant), len(articles))

    if not relevant:
        return

    with SessionLocal() as session:
        for art in relevant:
            row = SentimentNews(
                timestamp=art["published_at"],
                source=art["source"],
                title=art["title"],
                url=art["url"],
                sentiment=art["sentiment"],
                score=art["score"],
                relevance=art["relevance"],
            )
            session.add(row)
        session.commit()

    logger.info("Stored %d SentimentNews rows", len(relevant))

    # Compute weighted-average score (weight = relevance)
    total_weight = sum(a["relevance"] for a in relevant)
    weighted_score = sum(a["score"] * a["relevance"] for a in relevant) / total_weight
    avg_relevance = sum(a["relevance"] for a in relevant) / len(relevant)

    # Derive aggregate sentiment label from weighted score
    if weighted_score >= 0.1:
        agg_sentiment = "bullish"
    elif weighted_score <= -0.1:
        agg_sentiment = "bearish"
    else:
        agg_sentiment = "neutral"

    event = SentimentEvent(
        timestamp=datetime.now(tz=timezone.utc),
        source_type="news",
        sentiment=agg_sentiment,
        score=round(weighted_score, 4),
        relevance=round(avg_relevance, 4),
        summary=f"Aggregated from {len(relevant)} articles",
    )
    publish(_STREAM, event.model_dump())
    logger.info("Published SentimentEvent to stream '%s' (score=%.3f)", _STREAM, weighted_score)
