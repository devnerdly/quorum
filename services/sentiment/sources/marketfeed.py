"""Telegram @marketfeed channel scraper.

Scrapes the public web preview at https://t.me/s/marketfeed (no auth needed),
filters messages for crude-oil relevance, and uses Claude Haiku to score each
relevant message into the SentimentNews table.

@marketfeed posts breaking financial / geopolitical news that moves oil:
OPEC decisions, Iran/Israel/Hormuz events, US inventory surprises, sanctions,
ceasefires, drone strikes on energy infrastructure, etc.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests
import anthropic
from bs4 import BeautifulSoup
from sqlalchemy import select

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.sentiment import SentimentNews
from shared.redis_streams import publish
from shared.schemas.events import SentimentEvent

logger = logging.getLogger(__name__)

_CHANNEL_URL = "https://t.me/s/marketfeed"
_SOURCE_NAME = "telegram_marketfeed"
_STREAM = "sentiment.news"

# Keywords that gate which messages we send to Haiku for scoring. Pre-filtering
# saves Anthropic tokens — most @marketfeed posts are not oil-related.
_OIL_KEYWORDS = re.compile(
    r"\b(oil|crude|brent|wti|opec|opec\+|petroleum|barrel|refinery|"
    r"pipeline|tanker|hormuz|strait|saudi|aramco|iran|iraq|libya|venezuela|"
    r"russia|kuwait|uae|emirates|gas\s*field|drone\s+strike|sanction|"
    r"embargo|export|import|inventory|stockpile|spr|cushing|"
    r"price\s*cap|production\s*cut|refinery\s*outage|nord\s*stream|"
    r"red\s*sea|suez|houthi|natural\s*gas|lng)\b",
    re.IGNORECASE,
)

_CLASSIFY_PROMPT = """You are an oil-market analyst. Classify the following breaking-news headline for its impact on Brent crude oil prices.

HEADLINE:
{title}

Respond with ONLY a JSON object (no markdown, no extra text) with these exact keys:
  "sentiment": "bullish" | "bearish" | "neutral"
  "score":     float in [-1.0, +1.0]   (negative = bearish for oil price)
  "relevance": float in [0.0, 1.0]     (1.0 = directly moves the oil market)
  "reason":    string                  (one short sentence explaining)

Be strict with relevance: only score 0.7+ if the news directly affects oil supply/demand/risk premium."""


_anthropic_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    return _anthropic_client


def fetch_marketfeed_messages() -> list[dict[str, Any]]:
    """Scrape @marketfeed public preview and return parsed messages.

    Each dict has: ``url`` (canonical t.me URL), ``timestamp`` (UTC), ``title``.
    """
    logger.info("Fetching @marketfeed channel preview")
    response = requests.get(
        _CHANNEL_URL,
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0 (compatible; BrentBot/1.0)"},
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    messages: list[dict[str, Any]] = []

    for wrapper in soup.select(".tgme_widget_message_wrap"):
        msg = wrapper.select_one(".tgme_widget_message")
        if msg is None:
            continue

        post_id = msg.get("data-post")
        if not post_id:
            continue
        url = f"https://t.me/{post_id}"

        text_node = msg.select_one(".tgme_widget_message_text")
        if text_node is None:
            continue
        title = text_node.get_text(separator=" ", strip=True)
        if not title:
            continue

        time_node = msg.select_one("time.time")
        ts: datetime
        if time_node and time_node.get("datetime"):
            try:
                ts = datetime.fromisoformat(time_node["datetime"])
            except ValueError:
                ts = datetime.now(tz=timezone.utc)
        else:
            ts = datetime.now(tz=timezone.utc)

        messages.append({"url": url, "timestamp": ts, "title": title})

    logger.info("Parsed %d messages from @marketfeed", len(messages))
    return messages


def _is_oil_relevant(title: str) -> bool:
    return bool(_OIL_KEYWORDS.search(title))


def classify_message(title: str) -> dict[str, Any] | None:
    """Use Claude Haiku to classify a single headline."""
    try:
        response = _get_client().messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": _CLASSIFY_PROMPT.format(title=title)}],
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.MULTILINE)
        data = json.loads(text)
        return {
            "sentiment": str(data.get("sentiment", "neutral")).lower(),
            "score": float(data.get("score", 0.0)),
            "relevance": float(data.get("relevance", 0.0)),
            "reason": str(data.get("reason", "")),
        }
    except Exception:
        logger.exception("Haiku classification failed for: %s", title[:80])
        return None


def collect_and_store() -> None:
    """Scrape @marketfeed, score new oil-relevant messages, persist to DB."""
    try:
        messages = fetch_marketfeed_messages()
    except Exception:
        logger.exception("Failed to fetch @marketfeed")
        return

    if not messages:
        logger.warning("@marketfeed returned no messages")
        return

    # Deduplicate against URLs we've already stored
    urls = [m["url"] for m in messages]
    with SessionLocal() as session:
        existing = set(
            session.scalars(
                select(SentimentNews.url).where(SentimentNews.url.in_(urls))
            ).all()
        )

    new_messages = [m for m in messages if m["url"] not in existing]
    logger.info("@marketfeed: %d new messages (skipped %d duplicates)",
                len(new_messages), len(messages) - len(new_messages))

    if not new_messages:
        return

    # Pre-filter by keyword to save Haiku tokens
    relevant = [m for m in new_messages if _is_oil_relevant(m["title"])]
    logger.info("@marketfeed: %d/%d new messages match oil keywords",
                len(relevant), len(new_messages))

    if not relevant:
        return

    stored: list[dict[str, Any]] = []
    skipped = 0
    with SessionLocal() as session:
        for msg in relevant:
            classification = classify_message(msg["title"])
            if classification is None or classification["relevance"] < 0.3:
                skipped += 1
                continue

            row = SentimentNews(
                timestamp=msg["timestamp"],
                source=_SOURCE_NAME,
                title=msg["title"][:1000],
                url=msg["url"],
                sentiment=classification["sentiment"][:16],
                score=classification["score"],
                relevance=classification["relevance"],
            )
            session.add(row)
            stored.append({
                "title": msg["title"][:200],
                "url": msg["url"],
                "score": classification["score"],
                "relevance": classification["relevance"],
                "reason": classification["reason"],
            })
        session.commit()

    logger.info("@marketfeed: stored %d, skipped %d (low-relevance / errors)",
                len(stored), skipped)

    if not stored:
        return

    # Publish a SentimentEvent summarising this batch
    avg_score = sum(s["score"] * s["relevance"] for s in stored) / sum(
        s["relevance"] for s in stored
    )
    event = SentimentEvent(
        timestamp=datetime.now(tz=timezone.utc),
        source_type="news",
        sentiment="bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral",
        score=avg_score,
        relevance=1.0,
        summary=f"@marketfeed: {len(stored)} oil-relevant messages, avg score {avg_score:+.2f}",
    )
    publish(_STREAM, event.model_dump())
    logger.info("Published @marketfeed SentimentEvent (avg score %+.2f)", avg_score)
