"""Cross-asset context via Twelve Data (single paid feed).

Replaces the old Yahoo-based cross_assets.py. We track the same five
reference instruments we used to — DXY / SPX / Gold / BTC / VIX — but
route everything through Twelve Data for consistency and SLA.

Twelve Data Grow does NOT include raw index symbols (DXY, SPX, VIX),
so we use liquid ETF proxies that tightly track the underlying. The
correlation behaviour vs oil is indistinguishable from the real index
for our purposes (we care about direction and regime, not exact level).

Mapping:
  DXY  → UUP      (Invesco DB US Dollar Index Bullish Fund, NYSE)
  SPX  → SPY      (SPDR S&P 500 ETF, NYSE)
  GOLD → XAU/USD  (spot gold vs USD, forex)
  BTC  → BTC/USD  (Coinbase Pro, spot)
  VIX  → VIXY     (ProShares VIX Short-Term Futures ETF, CBOE)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests
from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)

_BASE = "https://api.twelvedata.com"

# Internal label -> Twelve Data symbol
SYMBOLS: dict[str, str] = {
    "DXY":  "UUP",
    "SPX":  "SPY",
    "GOLD": "XAU/USD",
    "BTC":  "BTC/USD",
    "VIX":  "VIXY",
}

# Twelve Data interval -> internal timeframe suffix
_INTERVAL_MAP: dict[str, str] = {
    "1h":   "1h",
    "1day": "1d",
}


def _parse_ts(datetime_str: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(datetime_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"unparseable datetime: {datetime_str!r}")


def _fetch_one(label: str, twelve_symbol: str, interval: str, outputsize: int) -> list[dict]:
    if not settings.twelve_api_key:
        return []
    try:
        r = requests.get(
            f"{_BASE}/time_series",
            params={
                "symbol": twelve_symbol,
                "interval": interval,
                "outputsize": outputsize,
                "apikey": settings.twelve_api_key,
                "format": "JSON",
                "timezone": "UTC",
            },
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
    except Exception as exc:
        logger.error("twelve cross-asset fetch failed (%s/%s): %s", label, interval, exc)
        return []

    if isinstance(payload, dict) and payload.get("status") == "error":
        logger.error(
            "twelve cross-asset API error (%s/%s): %s",
            label, interval, payload.get("message", payload),
        )
        return []

    values = payload.get("values") or []
    tf_label = _INTERVAL_MAP.get(interval, interval)
    records: list[dict] = []
    for row in values:
        try:
            records.append({
                "timestamp": _parse_ts(row["datetime"]),
                "source": "cross_asset",
                "timeframe": f"{label}:{tf_label}",
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]) if row.get("volume") else None,
            })
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("skipping malformed row for %s: %s (%s)", label, row, exc)
    return records


def collect_and_store(interval: str = "1h", outputsize: int = 200) -> None:
    """Fetch all 5 cross-asset symbols and upsert under source='cross_asset'."""
    all_records: list[dict] = []
    for label, twelve_sym in SYMBOLS.items():
        recs = _fetch_one(label, twelve_sym, interval, outputsize)
        if recs:
            all_records.extend(recs)
            logger.info(
                "twelve cross-asset: %d bars for %s (%s, %s)",
                len(recs), label, twelve_sym, interval,
            )

    if not all_records:
        return

    with SessionLocal() as session:
        stmt = pg_insert(OHLCV).values(all_records)
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
    logger.info("Upserted %d cross-asset rows (twelve, %s)", len(all_records), interval)
