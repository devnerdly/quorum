"""AI Brain service — orchestrates Haiku, Grok, and Opus to produce trading recommendations."""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from shared.redis_streams import subscribe, publish
from shared.schemas.events import RecommendationEvent

from agents.haiku import summarize_scores
from agents.grok import get_twitter_narrative
from agents.opus import synthesize_recommendation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

STREAM_IN = "analysis.scores"
STREAM_OUT = "signal.recommendation"
GROUP = "ai-brain"
CONSUMER = "ai-brain-1"


def process_scores(scores: dict) -> None:
    """Run the full AI pipeline for a given scores event and publish the result."""
    logger.info("Processing scores: unified=%s", scores.get("unified_score"))

    # --- Step 1: Haiku + Grok in parallel ---
    haiku_summary: str = ""
    grok_narrative: str = ""

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_haiku = executor.submit(summarize_scores, scores)
        future_grok = executor.submit(get_twitter_narrative)

        for future in as_completed([future_haiku, future_grok]):
            if future is future_haiku:
                try:
                    haiku_summary = future.result()
                    logger.info("Haiku summary ready (%d chars)", len(haiku_summary))
                except Exception:
                    logger.exception("Haiku agent raised an unexpected error")
                    haiku_summary = "Haiku summary unavailable."
            else:
                try:
                    grok_narrative = future.result()
                    logger.info("Grok narrative ready (%d chars)", len(grok_narrative))
                except Exception:
                    logger.exception("Grok agent raised an unexpected error")
                    grok_narrative = "Grok narrative unavailable."

    # --- Step 2: Opus sequentially ---
    rec = synthesize_recommendation(scores, haiku_summary, grok_narrative)
    logger.info(
        "Opus recommendation: action=%s confidence=%s",
        rec.get("action"),
        rec.get("confidence"),
    )

    # --- Step 3: Publish to Redis stream ---
    event = RecommendationEvent(
        timestamp=datetime.now(timezone.utc),
        action=rec.get("action", "WAIT"),
        unified_score=rec.get("unified_score"),
        opus_override_score=rec.get("opus_override_score"),
        confidence=rec.get("confidence"),
        entry_price=rec.get("entry_price"),
        stop_loss=rec.get("stop_loss"),
        take_profit=rec.get("take_profit"),
        haiku_summary=haiku_summary,
        grok_narrative=grok_narrative,
    )
    try:
        publish(STREAM_OUT, event.model_dump())
        logger.info("Published RecommendationEvent to %s", STREAM_OUT)
    except Exception:
        logger.exception("Failed to publish recommendation to Redis")


def main() -> None:
    logger.info("AI Brain service starting — listening on stream '%s'", STREAM_IN)

    for msg_id, data in subscribe(STREAM_IN, group=GROUP, consumer=CONSUMER, block=10_000):
        logger.info("Received scores event %s", msg_id)
        try:
            process_scores(data)
        except Exception:
            logger.exception("Failed to process scores event %s", msg_id)


if __name__ == "__main__":
    main()
