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
    from collectors.binance_metrics import (
        collect_all_metrics as bn_metrics_all,
        collect_funding_rate as bn_funding,
    )
    from collectors.binance_liquidations_ws import start_liquidations_ws
    from collectors.twelve_data_wti import collect_and_store as twelve_wti_collect
    from collectors.twelve_data_cross_assets import (
        collect_and_store as twelve_cross_assets_collect,
    )
    from collectors.shipping import collect_and_store as shipping_collect
    from collectors.portwatch import collect_and_store as portwatch_collect
    from collectors.cot import collect_and_store as cot_collect
    from collectors.jodi import collect_and_store as jodi_collect

    scheduler = BlockingScheduler(timezone="UTC")

    # --- Twelve Data WTI/USD — SINGLE canonical price source ---
    # Grow plan: 55 req/min, no daily limit, real-time commodity quotes,
    # SLA. Replaces Yahoo CL=F (delayed, 429s) and Binance CLUSDT
    # (TRADIFI perpetual drifts 1-3% from real NYMEX during off-hours).
    scheduler.add_job(
        safe_run, "interval", minutes=1, args=[twelve_wti_collect, "1min", 500],
        id="twelve_wti_1m", name="Twelve Data WTI 1-min",
        max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", minutes=5, args=[twelve_wti_collect, "5min", 500],
        id="twelve_wti_5m", name="Twelve Data WTI 5-min",
        max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", minutes=15, args=[twelve_wti_collect, "15min", 500],
        id="twelve_wti_15m", name="Twelve Data WTI 15-min",
        max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", hours=1, args=[twelve_wti_collect, "1h", 500],
        id="twelve_wti_1h", name="Twelve Data WTI 1-hour",
        max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", hours=6, args=[twelve_wti_collect, "1day", 200],
        id="twelve_wti_1d", name="Twelve Data WTI 1-day",
        max_instances=1, coalesce=True,
    )

    # --- Cross-asset context via Twelve Data (ETF proxies) ---
    # Same feed as the main price data, keeps the project on a single
    # paid data vendor. Symbols: UUP (DXY), SPY (SPX), XAU/USD (Gold),
    # BTC/USD (Bitcoin), VIXY (VIX).
    scheduler.add_job(
        safe_run, "interval", minutes=15, args=[twelve_cross_assets_collect, "1h", 200],
        id="twelve_cross_assets", name="Twelve Data cross-asset context",
        max_instances=1, coalesce=True,
    )

    # --- Binance derivatives metrics (unique Binance data — KEPT) ---
    # These are the ONE thing Twelve Data can't replace: funding rate,
    # open interest, long/short ratios, and the live liquidation stream.
    # We do NOT write Binance klines to OHLCV anymore — the chart and
    # all price decisions use Twelve Data only.
    scheduler.add_job(
        safe_run, "interval", minutes=5, args=[bn_metrics_all],
        id="binance_metrics", name="Binance derived metrics (OI/LSR/taker)",
        max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", minutes=30, args=[bn_funding, 500],
        id="binance_funding", name="Binance funding rate history",
        max_instances=1, coalesce=True,
    )

    # Liquidation stream — reconnecting WS worker in background thread
    start_liquidations_ws()

    # --- Yahoo Finance DISABLED — replaced by Binance CLUSDT (better data) ---
    # collectors/yahoo.py kept in repo as reference only. See commit that
    # migrated to Binance.

    # --- Macro / fundamental jobs (only the ones that actually work for free) ---
    from collectors.eia import collect_and_store as eia_collect
    from collectors.fred import collect_and_store as fred_collect

    scheduler.add_job(
        safe_run, "interval", hours=6, args=[eia_collect],
        id="eia", name="EIA crude inventories", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", hours=12, args=[fred_collect],
        id="fred", name="FRED macro series", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", hours=24, args=[cot_collect],
        id="cot", name="CFTC COT (cftc.gov)", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", hours=24, args=[jodi_collect],
        id="jodi", name="JODI Oil World", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", hours=24, args=[portwatch_collect],
        id="portwatch", name="IMF PortWatch (ArcGIS)", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        safe_run, "interval", hours=6, args=[shipping_collect],
        id="shipping", name="Datalastic AIS (skipped without key)", max_instances=1, coalesce=True,
    )

    # STILL DISABLED:
    #   - OPEC MOMR HTML (403 — Cloudflare/Akamai blocks all bot UAs)
    #     would require headless Playwright + JS execution to bypass.

    # Log all scheduled jobs
    logger.info("Scheduled jobs:")
    for job in scheduler.get_jobs():
        logger.info("  • %s (id=%s, trigger=%s)", job.name, job.id, job.trigger)

    # Warm up Twelve Data WTI (single canonical price feed)
    logger.info("Warming up Twelve Data WTI feed …")
    safe_run(twelve_wti_collect, "1min", 500)
    safe_run(twelve_wti_collect, "5min", 500)
    safe_run(twelve_wti_collect, "15min", 500)
    safe_run(twelve_wti_collect, "1h", 500)
    safe_run(twelve_wti_collect, "1day", 200)

    # Warm up Twelve Data cross-asset context (UUP, SPY, XAU/USD, BTC/USD, VIXY)
    logger.info("Warming up Twelve Data cross-asset feed …")
    safe_run(twelve_cross_assets_collect, "1h", 200)

    # Warm up Binance derivatives metrics (funding, OI, L/S ratios)
    logger.info("Warming up Binance derivatives metrics …")
    safe_run(bn_funding, 500)
    safe_run(bn_metrics_all)

    # Warm up macro / shipping collectors so the analyzer has fundamental
    # and shipping data on first cycle (instead of waiting hours).
    logger.info("Warming up macro and shipping collectors …")
    safe_run(eia_collect)
    safe_run(fred_collect)
    safe_run(cot_collect)
    safe_run(jodi_collect)
    safe_run(portwatch_collect)
    safe_run(shipping_collect)
    logger.info("Warm-up complete.")

    logger.info("Starting scheduler …")
    scheduler.start()


if __name__ == "__main__":
    main()
