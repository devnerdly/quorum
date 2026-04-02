"""Database initialisation: create tables, TimescaleDB extension, hypertables.

Run with:
    python -m shared.db_init
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from shared.models.base import Base, engine

logger = logging.getLogger(__name__)

# Tables that should become hypertables (they all have a 'timestamp' column).
_HYPERTABLES: list[str] = [
    "ohlcv",
    "macro_eia",
    "macro_cot",
    "macro_fred",
    "macro_jodi",
    "macro_opec",
    "sentiment_news",
    "sentiment_twitter",
    "analysis_scores",
    "ai_recommendations",
    "shipping_positions",
    "shipping_metrics",
]

# Compression policy: compress chunks older than this many days.
_COMPRESS_AFTER_DAYS = 30


def init_db() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    with engine.connect() as conn:
        # Enable the TimescaleDB extension (no-op if already enabled).
        logger.info("Enabling TimescaleDB extension …")
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
        conn.commit()

    # Create all ORM-mapped tables.
    logger.info("Creating tables …")
    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        for table in _HYPERTABLES:
            # create_hypertable raises an error if the table is already a
            # hypertable, so we use if_not_exists => TRUE.
            logger.info("Creating hypertable: %s", table)
            conn.execute(
                text(
                    f"SELECT create_hypertable('{table}', 'timestamp', "
                    f"if_not_exists => TRUE);"
                )
            )
            conn.commit()

            # Add a compression policy.
            logger.info("Adding compression policy for: %s (%d days)", table, _COMPRESS_AFTER_DAYS)
            try:
                conn.execute(
                    text(
                        f"ALTER TABLE {table} SET ("
                        f"timescaledb.compress, "
                        f"timescaledb.compress_orderby = 'timestamp DESC'"
                        f");"
                    )
                )
                conn.execute(
                    text(
                        f"SELECT add_compression_policy('{table}', "
                        f"INTERVAL '{_COMPRESS_AFTER_DAYS} days', "
                        f"if_not_exists => TRUE);"
                    )
                )
                conn.commit()
            except Exception as exc:
                # Non-fatal — compression may already be configured.
                logger.warning("Compression policy for %s skipped: %s", table, exc)
                conn.rollback()

    logger.info("Database initialisation complete.")


if __name__ == "__main__":
    init_db()
