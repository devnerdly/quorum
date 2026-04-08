"""Sentiment service — schedules RSS and Twitter/X sentiment collection."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.blocking import BlockingScheduler

from sources.rss import collect_and_store as rss_collect
from sources.twitter import collect_and_store as twitter_collect
from sources.marketfeed import collect_and_store as marketfeed_collect
from sources.marketfeed_summary import collect_and_store as marketfeed_summary_collect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def safe_run(fn, name: str) -> None:
    """Execute *fn* and log any exception without crashing the scheduler."""
    try:
        fn()
    except Exception:
        logger.exception("Job '%s' failed", name)


def main() -> None:
    scheduler = BlockingScheduler(timezone="UTC")

    # RSS news: every 30 minutes
    scheduler.add_job(
        safe_run,
        "interval",
        minutes=30,
        args=[rss_collect, "rss_news"],
        id="rss_news",
        next_run_time=datetime.now(tz=timezone.utc),  # fire immediately on startup  # do not run immediately on startup
    )

    # Twitter/X via Grok: every 15 minutes
    scheduler.add_job(
        safe_run,
        "interval",
        minutes=15,
        args=[twitter_collect, "twitter_sentiment"],
        id="twitter_sentiment",
        next_run_time=datetime.now(tz=timezone.utc),  # fire immediately on startup
    )

    # Telegram @marketfeed channel: every 5 minutes
    scheduler.add_job(
        safe_run,
        "interval",
        minutes=5,
        args=[marketfeed_collect, "marketfeed"],
        id="marketfeed",
        next_run_time=datetime.now(tz=timezone.utc),
    )

    # @marketfeed 5-min digest summary: runs 60 seconds AFTER scrape so the
    # digest sees the freshly-stored messages.
    scheduler.add_job(
        safe_run,
        "interval",
        minutes=5,
        args=[marketfeed_summary_collect, "marketfeed_summary"],
        id="marketfeed_summary",
        next_run_time=datetime.now(tz=timezone.utc) + timedelta(seconds=60),
    )

    logger.info(
        "Sentiment scheduler starting — RSS 30min, Twitter 15min, "
        "@marketfeed 5min, marketfeed_summary 5min"
    )
    scheduler.start()


if __name__ == "__main__":
    main()
