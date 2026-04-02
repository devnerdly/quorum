"""Data-collector service entry point.

Schedules periodic jobs to fetch Brent crude OHLCV data from Yahoo Finance
and Alpha Vantage, persist it to TimescaleDB, and publish events to Redis.
"""

from __future__ import annotations

import logging
import traceback
from typing import Callable

from apscheduler.schedulers.blocking import BlockingScheduler

from shared.db_init import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def safe_run(fn: Callable, *args, **kwargs) -> None:
    """Call *fn* with *args*/*kwargs*, logging any exception without crashing."""
    try:
        fn(*args, **kwargs)
    except Exception:
        logger.error("Job %s failed:\n%s", fn.__qualname__, traceback.format_exc())


def main() -> None:
    logger.info("Initialising database …")
    init_db()
    logger.info("Database initialised.")

    # Import collectors after DB is initialised (avoids import-time DB calls)
    from collectors.yahoo import collect_and_store as yf_collect
    from collectors.alpha_vantage import collect_and_store as av_collect

    scheduler = BlockingScheduler(timezone="UTC")

    # --- Yahoo Finance jobs ---
    # 1-minute bars: run every minute, fetch last day
    scheduler.add_job(
        safe_run,
        "interval",
        minutes=1,
        args=[yf_collect, "1m", "1d"],
        id="yahoo_1m",
        name="Yahoo 1-minute OHLCV",
        max_instances=1,
        coalesce=True,
    )

    # 1-hour bars: run every hour, fetch last 5 days
    scheduler.add_job(
        safe_run,
        "interval",
        hours=1,
        args=[yf_collect, "1h", "5d"],
        id="yahoo_1h",
        name="Yahoo 1-hour OHLCV",
        max_instances=1,
        coalesce=True,
    )

    # 1-day bars: run every 6 hours, fetch last 30 days
    scheduler.add_job(
        safe_run,
        "interval",
        hours=6,
        args=[yf_collect, "1d", "1mo"],
        id="yahoo_1d",
        name="Yahoo 1-day OHLCV",
        max_instances=1,
        coalesce=True,
    )

    # --- Alpha Vantage jobs ---
    # 5-minute bars: run every 5 minutes
    scheduler.add_job(
        safe_run,
        "interval",
        minutes=5,
        args=[av_collect, "5min"],
        id="av_5min",
        name="Alpha Vantage 5-minute OHLCV",
        max_instances=1,
        coalesce=True,
    )

    # Log all scheduled jobs
    logger.info("Scheduled jobs:")
    for job in scheduler.get_jobs():
        logger.info("  • %s (id=%s, trigger=%s)", job.name, job.id, job.trigger)

    logger.info("Starting scheduler …")
    scheduler.start()


if __name__ == "__main__":
    main()
