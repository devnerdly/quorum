"""Analyzer service — subscribes to prices.brent, computes scores, publishes to analysis.scores."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone

from shared.redis_streams import subscribe, publish
from shared.schemas.events import ScoresEvent

from indicators.technical import compute_technical_score
from indicators.fundamental import compute_fundamental_score
from indicators.scoring import compute_unified_score, get_latest_sentiment_score, store_scores

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

STREAM_IN = "prices.brent"
STREAM_OUT = "analysis.scores"
GROUP = "analyzer"
CONSUMER = "analyzer-1"

# Minimum seconds between analysis cycles (900s = 15 minutes per spec).
MIN_CYCLE_INTERVAL_SECONDS = 900

# Last time run_analysis actually published a scores event.
_last_cycle_ts: float = 0.0

# Set to True when a price event arrives but the throttle blocks the cycle.
# A background watchdog thread will re-trigger the cycle when the throttle expires.
_pending_cycle: bool = False
_pending_lock = threading.Lock()


def _watchdog() -> None:
    """Background thread: re-run analysis when a pending cycle is overdue."""
    while True:
        time.sleep(30)
        with _pending_lock:
            pending = _pending_cycle
        if pending:
            elapsed = time.time() - _last_cycle_ts
            if elapsed >= MIN_CYCLE_INTERVAL_SECONDS:
                logger.info("Watchdog: pending cycle overdue — running deferred analysis")
                try:
                    run_analysis(force=True)
                except Exception:
                    logger.exception("Watchdog deferred analysis cycle failed")


def run_analysis(force: bool = False) -> None:
    """Compute all scores and publish the result.

    Throttled to MIN_CYCLE_INTERVAL_SECONDS unless *force=True*. Skips
    publishing when every score is None (cold start — no data yet).
    When throttled, sets _pending_cycle=True so the watchdog can re-trigger.
    """
    global _last_cycle_ts, _pending_cycle
    now_ts = time.time()
    if not force and (now_ts - _last_cycle_ts) < MIN_CYCLE_INTERVAL_SECONDS:
        remaining = MIN_CYCLE_INTERVAL_SECONDS - (now_ts - _last_cycle_ts)
        logger.info("Throttled — %.0fs until next cycle (deferred cycle queued)", remaining)
        with _pending_lock:
            _pending_cycle = True
        return

    logger.info("Running analysis cycle")

    technical = compute_technical_score()
    fundamental = compute_fundamental_score()
    sentiment_shipping = get_latest_sentiment_score()
    unified = compute_unified_score(technical, fundamental, sentiment_shipping)

    # Skip publishing when everything is None (cold start). The AI brain
    # would just burn tokens producing an "unable to analyse" message.
    if technical is None and fundamental is None and sentiment_shipping is None and unified is None:
        logger.info("All scores None — skipping publish (cold start)")
        _last_cycle_ts = now_ts
        return

    logger.info(
        "Scores — technical=%s fundamental=%s sentiment=%s unified=%s",
        f"{technical:.1f}" if technical is not None else "N/A",
        f"{fundamental:.1f}" if fundamental is not None else "N/A",
        f"{sentiment_shipping:.1f}" if sentiment_shipping is not None else "N/A",
        f"{unified:.1f}" if unified is not None else "N/A",
    )

    # Persist to DB
    try:
        store_scores(technical, fundamental, sentiment_shipping, unified)
    except Exception:
        logger.exception("Failed to persist scores to DB")

    # Publish to Redis stream
    event = ScoresEvent(
        timestamp=datetime.now(timezone.utc),
        technical_score=technical,
        fundamental_score=fundamental,
        sentiment_score=sentiment_shipping,
        shipping_score=None,
        unified_score=unified,
    )
    try:
        publish(STREAM_OUT, event.model_dump())
        logger.info("Published ScoresEvent to %s", STREAM_OUT)
        _last_cycle_ts = now_ts
        with _pending_lock:
            _pending_cycle = False
    except Exception:
        logger.exception("Failed to publish scores to Redis")


def main() -> None:
    logger.info("Analyzer service starting — listening on stream '%s'", STREAM_IN)

    # Start background watchdog to handle deferred cycles when throttle blocks
    watchdog_thread = threading.Thread(target=_watchdog, name="analyzer-watchdog", daemon=True)
    watchdog_thread.start()
    logger.info("Watchdog thread started (checks every 30s for deferred cycles)")

    # Run an initial analysis cycle on startup
    try:
        run_analysis()
    except Exception:
        logger.exception("Initial analysis cycle failed")

    # Subscribe to price events and re-run analysis on each new bar
    for msg_id, data in subscribe(STREAM_IN, group=GROUP, consumer=CONSUMER, block=10_000):
        logger.info("Received price event %s — triggering analysis", msg_id)
        try:
            run_analysis()
        except Exception:
            logger.exception("Analysis cycle failed for message %s", msg_id)


if __name__ == "__main__":
    main()
