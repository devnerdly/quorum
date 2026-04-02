"""Sentiment service — schedules RSS and Twitter/X sentiment collection."""

from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from sources.rss import collect_and_store as rss_collect
from sources.twitter import collect_and_store as twitter_collect

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
        next_run_time=None,  # do not run immediately on startup
    )

    # Twitter/X via Grok: every 15 minutes
    scheduler.add_job(
        safe_run,
        "interval",
        minutes=15,
        args=[twitter_collect, "twitter_sentiment"],
        id="twitter_sentiment",
        next_run_time=None,
    )

    logger.info("Sentiment scheduler starting — RSS every 30 min, Twitter every 15 min")
    scheduler.start()


if __name__ == "__main__":
    main()
