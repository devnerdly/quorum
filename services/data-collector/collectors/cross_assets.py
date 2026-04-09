"""Cross-asset collector for correlation/regime context.

Tracks a small set of instruments whose relationship to oil tells us
about broader market regime:

  DX-Y.NYB  — US Dollar Index (DXY). Inverse correlation with oil.
  ^SPX      — S&P 500. Proxy for risk-on/off.
  GC=F      — Gold front-month. Safe haven.
  BTC-USD   — Bitcoin. Crypto-wide risk sentiment.
  ^VIX      — VIX. Equity fear gauge.

Writes into the shared OHLCV table with source="yahoo" and a ticker-
specific timeframe label (e.g. "DXY:1H") so the dashboard chart, which
queries source="binance", never picks them up by accident.
"""

from __future__ import annotations

import logging
from datetime import timezone

import yfinance as yf
from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)


# Symbol -> (yfinance ticker, internal label)
SYMBOLS: dict[str, str] = {
    "DXY": "DX-Y.NYB",
    "SPX": "^SPX",
    "GOLD": "GC=F",
    "BTC": "BTC-USD",
    "VIX": "^VIX",
}


def _fetch(ticker: str, interval: str = "1h", period: str = "5d") -> list[dict]:
    try:
        df = yf.download(ticker, interval=interval, period=period, progress=False, auto_adjust=True)
    except Exception as exc:
        logger.warning("yfinance download failed for %s: %s", ticker, exc)
        return []

    if df.empty:
        return []
    if hasattr(df.columns, "levels"):
        df.columns = df.columns.get_level_values(0)

    records: list[dict] = []
    for ts, row in df.iterrows():
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            aware_ts = ts.to_pydatetime()
        else:
            aware_ts = ts.to_pydatetime().replace(tzinfo=timezone.utc)
        records.append({
            "timestamp": aware_ts,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row["Volume"]) if "Volume" in row and row["Volume"] is not None else None,
        })
    return records


def collect_and_store(interval: str = "1h", period: str = "5d") -> None:
    """Fetch all cross-asset symbols and upsert under source='cross_asset'."""
    all_records: list[dict] = []
    for label, ticker in SYMBOLS.items():
        recs = _fetch(ticker, interval=interval, period=period)
        if not recs:
            continue
        for r in recs:
            r["source"] = "cross_asset"
            r["timeframe"] = f"{label}:{interval}"
            all_records.append(r)
        logger.info("Fetched %d cross-asset bars for %s (%s)", len(recs), label, interval)

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

    logger.info("Upserted %d cross-asset rows (%s)", len(all_records), interval)
