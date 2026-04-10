"""Dynamic range bias — where is price within its recent range?

Computes a 30-day rolling range from daily high/low bars and returns
a position score (0% = at the bottom, 100% = at the top) plus a
directional bias that tells the bot:

  - Near the bottom (0-25%): favor LONG, refuse SHORT
  - Lower-mid (25-40%): mildly favor LONG
  - Mid-range (40-60%): neutral, both sides OK
  - Upper-mid (60-75%): mildly favor SHORT
  - Near the top (75-100%): favor SHORT, refuse LONG

This prevents the bot from going LONG near range highs (where mean-
reversion risk is highest) or SHORT near range lows. The range adapts
dynamically as the market trends — it's always the last 30 calendar
days of price action.

Also computes key structural levels: range high, range low, midpoint,
and quartile boundaries — these are passed to Opus so it can reference
them in its reasoning ("price at $98.50 is in the top 20% of the 30-day
range $94.20-$99.10").
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV
from shared.position_manager import get_current_price

logger = logging.getLogger(__name__)

# How many days of history to use for the range
RANGE_LOOKBACK_DAYS = 30

# Percentile thresholds for bias zones
STRONG_LOW_PCT = 25    # below this = strongly favor LONG
MILD_LOW_PCT = 40      # 25-40 = mildly favor LONG
MILD_HIGH_PCT = 60     # 60-75 = mildly favor SHORT
STRONG_HIGH_PCT = 75   # above this = strongly favor SHORT


def compute_range_bias() -> dict:
    """Return the current range position, bias, and structural levels.

    Reads 30 days of daily (or 1h if daily unavailable) bars from the
    OHLCV table to compute the rolling high/low range, then locates
    the current price within it.

    Returns a dict with:
      range_high, range_low, range_mid — the 30-day boundaries
      current_price — latest price
      position_pct — 0-100, where current price sits in the range
      bias — "strong_long" | "mild_long" | "neutral" | "mild_short" | "strong_short"
      bias_score — -100 to +100 (negative = bearish bias, positive = bullish)
      should_refuse_long — True if price is in the top 25% (don't go long here)
      should_refuse_short — True if price is in the bottom 25% (don't go short here)
      q1, q2, q3 — quartile price levels for reference
    """
    current_price = get_current_price()
    if current_price is None:
        return {"error": "no current price", "bias": "neutral", "bias_score": 0}

    since = datetime.now(tz=timezone.utc) - timedelta(days=RANGE_LOOKBACK_DAYS)

    with SessionLocal() as session:
        # Try daily bars first (most stable)
        bars = (
            session.query(OHLCV)
            .filter(
                OHLCV.source == "twelve",
                OHLCV.timeframe == "1day",
                OHLCV.timestamp >= since,
            )
            .order_by(OHLCV.timestamp.asc())
            .all()
        )

        # Fall back to 1h bars if no daily data
        if len(bars) < 5:
            bars = (
                session.query(OHLCV)
                .filter(
                    OHLCV.source == "twelve",
                    OHLCV.timeframe.in_(["1h", "1H"]),
                    OHLCV.timestamp >= since,
                )
                .order_by(OHLCV.timestamp.asc())
                .all()
            )

        # Last resort: use 5min bars from last 7 days
        if len(bars) < 20:
            since_short = datetime.now(tz=timezone.utc) - timedelta(days=7)
            bars = (
                session.query(OHLCV)
                .filter(
                    OHLCV.source == "twelve",
                    OHLCV.timeframe == "5min",
                    OHLCV.timestamp >= since_short,
                )
                .order_by(OHLCV.timestamp.asc())
                .all()
            )

    if len(bars) < 10:
        return {
            "error": f"insufficient bars ({len(bars)})",
            "bias": "neutral",
            "bias_score": 0,
            "current_price": current_price,
        }

    # Compute the range from actual highs/lows (more accurate than closes)
    range_high = max(b.high for b in bars)
    range_low = min(b.low for b in bars)
    range_width = range_high - range_low

    if range_width <= 0:
        return {
            "error": "zero range width",
            "bias": "neutral",
            "bias_score": 0,
            "current_price": current_price,
        }

    # Where is current price in the range? 0% = at low, 100% = at high
    position_pct = ((current_price - range_low) / range_width) * 100
    position_pct = max(0.0, min(100.0, position_pct))

    # Quartile levels
    q1 = range_low + range_width * 0.25
    q2 = range_low + range_width * 0.50  # midpoint
    q3 = range_low + range_width * 0.75

    # Determine bias
    if position_pct <= STRONG_LOW_PCT:
        bias = "strong_long"
        bias_score = 50 + (STRONG_LOW_PCT - position_pct) * 2  # 50-100
    elif position_pct <= MILD_LOW_PCT:
        bias = "mild_long"
        bias_score = 20 + (MILD_LOW_PCT - position_pct)  # 20-35
    elif position_pct <= MILD_HIGH_PCT:
        bias = "neutral"
        bias_score = 0
    elif position_pct <= STRONG_HIGH_PCT:
        bias = "mild_short"
        bias_score = -(20 + (position_pct - MILD_HIGH_PCT))  # -20 to -35
    else:
        bias = "strong_short"
        bias_score = -(50 + (position_pct - STRONG_HIGH_PCT) * 2)  # -50 to -100

    bias_score = max(-100, min(100, round(bias_score)))

    return {
        "current_price": round(current_price, 3),
        "range_high": round(range_high, 3),
        "range_low": round(range_low, 3),
        "range_mid": round(q2, 3),
        "range_width": round(range_width, 3),
        "range_width_pct": round((range_width / current_price) * 100, 2),
        "position_pct": round(position_pct, 1),
        "q1": round(q1, 3),
        "q2": round(q2, 3),
        "q3": round(q3, 3),
        "bias": bias,
        "bias_score": bias_score,
        "should_refuse_long": position_pct >= STRONG_HIGH_PCT,
        "should_refuse_short": position_pct <= STRONG_LOW_PCT,
        "lookback_days": RANGE_LOOKBACK_DAYS,
        "bars_used": len(bars),
    }


def should_allow_entry(side: str) -> tuple[bool, str]:
    """Quick check: should the bot open a campaign in this direction?

    Returns (allowed, reason). Used as a hard gate in ai-brain's
    _handle_campaign_signal before opening any new campaign.
    """
    rb = compute_range_bias()
    if rb.get("error"):
        return True, "range bias unavailable — allowing"

    pos = rb["position_pct"]
    side_upper = side.upper()

    if side_upper == "LONG" and rb["should_refuse_long"]:
        return False, (
            f"BLOCKED: price ${rb['current_price']:.2f} is at {pos:.0f}% of 30-day range "
            f"(${rb['range_low']:.2f}-${rb['range_high']:.2f}) — top quartile, "
            f"mean-reversion risk too high for LONG"
        )

    if side_upper == "SHORT" and rb["should_refuse_short"]:
        return False, (
            f"BLOCKED: price ${rb['current_price']:.2f} is at {pos:.0f}% of 30-day range "
            f"(${rb['range_low']:.2f}-${rb['range_high']:.2f}) — bottom quartile, "
            f"mean-reversion risk too high for SHORT"
        )

    return True, (
        f"price at {pos:.0f}% of range (${rb['range_low']:.2f}-${rb['range_high']:.2f}), "
        f"bias={rb['bias']}"
    )
