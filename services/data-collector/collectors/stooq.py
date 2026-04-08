"""Stooq.com snapshot collector for ICE Brent crude oil (CB.F).

Stooq publishes a free per-symbol CSV snapshot endpoint:
    https://stooq.com/q/l/?s=cb.f&f=sd2t2ohlcv&h&e=csv

CB.F == ICE Brent Crude Oil front-month futures, which matches the contract
that XTB and most CFD brokers price their Brent CFD against. Yahoo's BZ=F is
the NYMEX "Brent Last Day Financial" derivative which can drift $0.30-$1.00
from the actual ICE front month.

IMPORTANT: Stooq's snapshot returns *daily* OHLC values (the day's running
open/high/low) plus a live close. We CANNOT use the open/high/low fields as
1-minute bar values — they would create fake $5+ wicks. Instead we synthesise
a flat tick bar where open=high=low=close=current_price, aligned to the
top of each minute. The snapshot's Time field is in CET (Warsaw time) — we
ignore it and use the live UTC poll time as the bar timestamp.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone

import requests
from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV
from shared.redis_streams import publish
from shared.schemas.events import PriceEvent

logger = logging.getLogger(__name__)

_STOOQ_URL = "https://stooq.com/q/l/"
_SYMBOL = "cb.f"  # ICE Brent Crude front-month
_STREAM = "prices.brent"
_SOURCE = "stooq"


def fetch_stooq_snapshot() -> dict | None:
    """Return the current ICE Brent snapshot from Stooq, or None on failure."""
    params = {
        "s": _SYMBOL,
        "f": "sd2t2ohlcv",  # symbol, date, time, OHLC, volume
        "h": "",            # include header
        "e": "csv",         # CSV format
    }
    try:
        response = requests.get(_STOOQ_URL, params=params, timeout=15)
        response.raise_for_status()
    except Exception as exc:
        logger.error("Stooq fetch failed: %s", exc)
        return None

    reader = csv.DictReader(io.StringIO(response.text))
    rows = list(reader)
    if not rows:
        logger.warning("Stooq returned empty CSV body")
        return None

    row = rows[0]
    # Stooq sometimes returns "N/D" placeholders when the market is closed
    if row.get("Close") in (None, "", "N/D"):
        logger.info("Stooq snapshot has no usable values (market likely closed)")
        return None

    try:
        close = float(row["Close"])
    except (KeyError, ValueError) as exc:
        logger.warning("Stooq close parse error: %s — row=%s", exc, row)
        return None

    # Use UTC poll time floored to the minute as the bar timestamp.
    # Stooq's Date+Time fields are in Warsaw time; we ignore them to avoid
    # timezone bugs and just align to the minute we received the snapshot.
    now = datetime.now(tz=timezone.utc)
    ts = now.replace(second=0, microsecond=0)

    # Tick bar: flat OHLC = current price (Stooq's snapshot O/H/L are *daily*
    # running values, not minute-level — we MUST NOT use them as wick values).
    return {
        "timestamp": ts,
        "source": _SOURCE,
        "timeframe": "1min",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 0.0,
    }


def collect_and_store() -> None:
    """Fetch current ICE Brent snapshot, upsert to DB, publish to Redis."""
    record = fetch_stooq_snapshot()
    if record is None:
        return

    with SessionLocal() as session:
        stmt = pg_insert(OHLCV).values([record])
        stmt = stmt.on_conflict_do_update(
            index_elements=["source", "timeframe", "timestamp"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
            },
        )
        session.execute(stmt)
        session.commit()

    logger.info(
        "Upserted Stooq ICE Brent snapshot: close=$%.2f at %s",
        record["close"],
        record["timestamp"].isoformat(),
    )

    event = PriceEvent(**record)
    publish(_STREAM, event.model_dump())
