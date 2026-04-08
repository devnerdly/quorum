"""Deep Dive entry-analysis chat-tool plugin.

Bundles many sub-queries (price action, technicals, news, account state,
correlation, upcoming events, SL/TP) into a single server-side report so
the LLM does not need to make 8 separate round-trip tool calls.

Integration contract
--------------------
PLUGIN_TOOLS : list[dict]
    Anthropic tool schema to be merged into the main TOOLS list.

execute(name, tool_input) -> dict | None
    Dispatch a tool by name.  Returns None for unhandled names so the
    main dispatcher can fall through to other plugins / built-ins.
"""

from __future__ import annotations

import logging
import math
import statistics
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anthropic tool schema
# ---------------------------------------------------------------------------

PLUGIN_TOOLS: list[dict] = [
    {
        "name": "deep_dive_entry_analysis",
        "description": (
            "Run a comprehensive entry-decision analysis for Brent crude. Bundles current "
            "market state, recent news, support/resistance, ATR-based SL/TP, VWAP, DXY "
            "correlation, upcoming events, account state, and open campaigns into a single "
            "structured report. Use this when the user asks 'should I enter', 'is this a good "
            "entry', 'what do you think right now' — instead of making 8 separate tool calls."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "side": {
                    "type": "string",
                    "enum": ["LONG", "SHORT", "EITHER"],
                    "description": (
                        "The direction the user is considering. "
                        "Use EITHER if the user is undecided."
                    ),
                },
                "focus_hours": {
                    "type": "integer",
                    "default": 4,
                    "description": "How many hours of recent news/prices to examine",
                },
            },
            "required": ["side"],
        },
    }
]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def execute(name: str, tool_input: dict) -> dict | None:
    """Dispatch a tool call to its implementation.

    Returns a serialisable dict for handled tools.
    Returns None for unhandled tool names so the main dispatcher can fall
    through to other plugins or built-ins.
    """
    if name == "deep_dive_entry_analysis":
        return _deep_dive(**tool_input)
    return None


# ---------------------------------------------------------------------------
# Main implementation
# ---------------------------------------------------------------------------

def _deep_dive(side: str = "EITHER", focus_hours: int = 4) -> dict:
    """Comprehensive entry-decision analysis bundled into one server-side call."""
    now = datetime.now(tz=timezone.utc)
    now_iso = now.isoformat()
    side = side.upper()

    raw_data: dict = {}

    # ------------------------------------------------------------------
    # 1. Current market state (price + scores + account + open campaigns)
    # ------------------------------------------------------------------
    market_state: dict = {}
    try:
        from chat_tools import _get_current_market_state
        market_state = _get_current_market_state()
        raw_data["market_state"] = market_state
    except Exception as exc:
        logger.exception("deep_dive: _get_current_market_state failed")
        market_state = {"error": str(exc)}
        raw_data["market_state"] = market_state

    current_price: float | None = market_state.get("current_price")
    scores_raw: dict = market_state.get("scores") or {}

    # ------------------------------------------------------------------
    # 2. Price action — last 60 min of 1-min bars
    # ------------------------------------------------------------------
    price_action: dict = {}
    try:
        price_action = _compute_price_action(now, focus_hours)
        raw_data["price_action_raw"] = price_action
        # Update current_price from freshest bar if market_state missed it
        if current_price is None and "current_price" in price_action:
            current_price = price_action["current_price"]
    except Exception as exc:
        logger.exception("deep_dive: _compute_price_action failed")
        price_action = {"error": str(exc)}

    # ------------------------------------------------------------------
    # 3. Technical levels — S/R, VWAP, pivot points, SL/TP
    # ------------------------------------------------------------------
    sr_data: dict = {}
    vwap_data: dict = {}
    pivot_data: dict = {}
    sl_tp_data: dict = {}

    try:
        from plugin_analytics import _get_support_resistance
        sr_data = _get_support_resistance(timeframe="1H", lookback_bars=100)
        raw_data["support_resistance"] = sr_data
    except Exception as exc:
        logger.exception("deep_dive: _get_support_resistance failed")
        sr_data = {"error": str(exc)}

    try:
        from plugin_analytics import _get_vwap
        vwap_data = _get_vwap(timeframe="1H", hours=24)
        raw_data["vwap"] = vwap_data
    except Exception as exc:
        logger.exception("deep_dive: _get_vwap failed")
        vwap_data = {"error": str(exc)}

    try:
        from plugin_analytics import _get_pivot_points
        pivot_data = _get_pivot_points()
        raw_data["pivot_points"] = pivot_data
    except Exception as exc:
        logger.exception("deep_dive: _get_pivot_points failed")
        pivot_data = {"error": str(exc)}

    # Determine effective side for SL/TP (if EITHER, use LONG as baseline)
    sl_tp_side = side if side in ("LONG", "SHORT") else "LONG"
    try:
        from plugin_analytics import _compute_optimal_sl_tp
        entry_for_sltp = current_price or 0.0
        sl_tp_data = _compute_optimal_sl_tp(
            side=sl_tp_side,
            entry_price=entry_for_sltp,
            method="atr",
            atr_multiplier_sl=1.5,
            atr_multiplier_tp=2.5,
        )
        raw_data["sl_tp"] = sl_tp_data
    except Exception as exc:
        logger.exception("deep_dive: _compute_optimal_sl_tp failed")
        sl_tp_data = {"error": str(exc)}

    # ------------------------------------------------------------------
    # 4. News & upcoming events
    # ------------------------------------------------------------------
    news_data: dict = {}
    events_data: dict = {}

    try:
        from chat_tools import _query_marketfeed
        news_data = _query_marketfeed(hours=focus_hours)
        raw_data["marketfeed"] = news_data
    except Exception as exc:
        logger.exception("deep_dive: _query_marketfeed failed")
        news_data = {"error": str(exc)}

    try:
        from plugin_analytics import _get_upcoming_events
        events_data = _get_upcoming_events(days=2)
        raw_data["upcoming_events"] = events_data
    except Exception as exc:
        logger.exception("deep_dive: _get_upcoming_events failed")
        events_data = {"error": str(exc)}

    # ------------------------------------------------------------------
    # 5. Account context
    # ------------------------------------------------------------------
    account_data: dict = {}
    campaigns_data: dict = {}

    try:
        from chat_tools import _get_account_state
        account_data = _get_account_state()
        raw_data["account"] = account_data
    except Exception as exc:
        logger.exception("deep_dive: _get_account_state failed")
        account_data = {"error": str(exc)}

    try:
        from chat_tools import _get_campaigns
        campaigns_data = _get_campaigns(status="open")
        raw_data["campaigns"] = campaigns_data
    except Exception as exc:
        logger.exception("deep_dive: _get_campaigns failed")
        campaigns_data = {"error": str(exc)}

    # ------------------------------------------------------------------
    # 6. DXY correlation (24h window)
    # ------------------------------------------------------------------
    corr_data: dict = {}
    try:
        from plugin_analytics import _get_correlation
        corr_data = _get_correlation(hours=24)
        raw_data["correlation"] = corr_data
    except Exception as exc:
        logger.exception("deep_dive: _get_correlation failed")
        corr_data = {"error": str(exc)}

    # ------------------------------------------------------------------
    # 7. Assemble sub-sections
    # ------------------------------------------------------------------

    # --- Scores ---
    scores_section = _build_scores_section(scores_raw)

    # --- Technical levels ---
    technical_levels = _build_technical_levels(
        sr_data, vwap_data, pivot_data, current_price
    )

    # --- SL/TP section ---
    suggested_sl_tp = _build_sl_tp_section(sl_tp_data)

    # --- News context ---
    news_context = _build_news_context(news_data, events_data, focus_hours)

    # --- Account context ---
    account_context = _build_account_context(
        account_data, campaigns_data, side, current_price
    )

    # --- Correlation ---
    correlation = _build_correlation_section(corr_data)

    # --- Price action section ---
    pa_section = _build_price_action_section(price_action)

    # ------------------------------------------------------------------
    # 8. Verdict synthesis
    # ------------------------------------------------------------------
    verdict = _synthesize_verdict(
        side=side,
        current_price=current_price,
        scores_raw=scores_raw,
        technical_levels=technical_levels,
        suggested_sl_tp=suggested_sl_tp,
        news_context=news_context,
        account_context=account_context,
        correlation=correlation,
    )

    return {
        "side_analyzed": side,
        "timestamp": now_iso,
        "current_price": current_price,
        "price_action": pa_section,
        "scores": scores_section,
        "technical_levels": technical_levels,
        "suggested_sl_tp": suggested_sl_tp,
        "news_context": news_context,
        "account_context": account_context,
        "correlation": correlation,
        "verdict": verdict,
        "raw_data": raw_data,
    }


# ---------------------------------------------------------------------------
# Sub-section builders
# ---------------------------------------------------------------------------

def _compute_price_action(now: datetime, focus_hours: int) -> dict:
    """Fetch recent 1-min bars and compute price-action metrics."""
    from shared.models.base import SessionLocal
    from shared.models.ohlcv import OHLCV
    from sqlalchemy import desc

    lookback_hours = max(focus_hours, 24) + 1  # enough for all comparisons
    since = now - timedelta(hours=lookback_hours)

    with SessionLocal() as session:
        rows = (
            session.query(OHLCV)
            .filter(OHLCV.timeframe == "1min", OHLCV.timestamp >= since)
            .order_by(OHLCV.timestamp)
            .all()
        )
        bars = [
            {
                "ts": r.timestamp if r.timestamp.tzinfo else r.timestamp.replace(tzinfo=timezone.utc),
                "close": r.close,
            }
            for r in rows
        ]

    if not bars:
        return {"error": "no 1min bars available"}

    current_price = bars[-1]["close"]
    current_ts = bars[-1]["ts"]

    def _price_at_offset(hours_ago: int) -> float | None:
        target = current_ts - timedelta(hours=hours_ago)
        # Find the bar closest to (and not after) target
        candidates = [b for b in bars if b["ts"] <= target]
        if not candidates:
            return None
        return candidates[-1]["close"]

    def _fmt_diff(p_now: float, p_then: float | None) -> str:
        if p_then is None:
            return "n/a"
        diff = p_now - p_then
        pct = (diff / p_then) * 100
        sign = "+" if diff >= 0 else ""
        return f"{sign}{diff:.2f} ({sign}{pct:.2f}%)"

    p1h = _price_at_offset(1)
    p4h = _price_at_offset(4)
    p24h = _price_at_offset(24)

    # Last 20 bars for volatility
    last_20_closes = [b["close"] for b in bars[-20:]]
    volatility: float | None = None
    if len(last_20_closes) >= 2:
        try:
            volatility = round(statistics.stdev(last_20_closes), 4)
        except Exception:
            volatility = None

    # Last 15 minutes trend
    last_15m_bars = [
        b for b in bars
        if b["ts"] >= current_ts - timedelta(minutes=15)
    ]
    last_15m_trend = "flat"
    if len(last_15m_bars) >= 2:
        delta = last_15m_bars[-1]["close"] - last_15m_bars[0]["close"]
        if delta > 0.05:
            last_15m_trend = "up"
        elif delta > 0.01:
            last_15m_trend = "slight up"
        elif delta < -0.05:
            last_15m_trend = "down"
        elif delta < -0.01:
            last_15m_trend = "slight down"

    # Hourly trend (last 60 min)
    last_60m_bars = [
        b for b in bars
        if b["ts"] >= current_ts - timedelta(minutes=60)
    ]
    hourly_trend = "flat"
    if len(last_60m_bars) >= 2:
        delta = last_60m_bars[-1]["close"] - last_60m_bars[0]["close"]
        if delta > 0.10:
            hourly_trend = "bullish"
        elif delta > 0.02:
            hourly_trend = "slight bullish"
        elif delta < -0.10:
            hourly_trend = "bearish"
        elif delta < -0.02:
            hourly_trend = "slight bearish"

    return {
        "current_price": round(current_price, 3),
        "current_ts": current_ts.isoformat(),
        "vs_1h_ago": _fmt_diff(current_price, p1h),
        "vs_4h_ago": _fmt_diff(current_price, p4h),
        "vs_24h_ago": _fmt_diff(current_price, p24h),
        "hourly_trend": hourly_trend,
        "last_15m_trend": last_15m_trend,
        "volatility_20bar": volatility,
        "bar_count_used": len(bars),
    }


def _build_price_action_section(price_action: dict) -> dict:
    if "error" in price_action:
        return price_action
    return {
        "vs_1h_ago": price_action.get("vs_1h_ago", "n/a"),
        "vs_4h_ago": price_action.get("vs_4h_ago", "n/a"),
        "vs_24h_ago": price_action.get("vs_24h_ago", "n/a"),
        "hourly_trend": price_action.get("hourly_trend", "unknown"),
        "last_15m_trend": price_action.get("last_15m_trend", "unknown"),
        "volatility_20bar": price_action.get("volatility_20bar"),
    }


def _build_scores_section(scores_raw: dict) -> dict:
    if not scores_raw:
        return {"error": "no scores available"}

    tech = scores_raw.get("technical")
    fund = scores_raw.get("fundamental")
    sent = scores_raw.get("sentiment")
    ship = scores_raw.get("shipping")
    uni = scores_raw.get("unified")

    # Interpretation
    interp = "insufficient data"
    if uni is not None:
        if uni > 30:
            interp = "bullish overall"
        elif uni > 10:
            interp = "mildly bullish"
        elif uni > -10:
            interp = "near-neutral"
        elif uni > -30:
            interp = "mildly bearish"
        else:
            interp = "bearish overall"

        if fund is not None and abs(fund) > 40:
            direction = "bearish" if fund < 0 else "bullish"
            interp += f" with strong {direction} fundamentals"

    return {
        "technical": tech,
        "fundamental": fund,
        "sentiment": sent,
        "shipping": ship,
        "unified": uni,
        "interpretation": interp,
    }


def _build_technical_levels(
    sr_data: dict,
    vwap_data: dict,
    pivot_data: dict,
    current_price: float | None,
) -> dict:
    result: dict = {}

    # VWAP
    if "error" not in vwap_data:
        result["vwap_24h"] = vwap_data.get("vwap")
        result["distance_from_vwap_pct"] = vwap_data.get("distance_pct")
        result["price_vs_vwap"] = vwap_data.get("price_vs_vwap")
    else:
        result["vwap_24h"] = {"error": vwap_data.get("error")}

    # Support / resistance
    if "error" not in sr_data:
        supports = sr_data.get("supports", [])
        resistances = sr_data.get("resistances", [])
        result["nearest_support"] = [s["price"] for s in supports[:2]]
        result["nearest_resistance"] = [r["price"] for r in resistances[:2]]
    else:
        result["nearest_support"] = {"error": sr_data.get("error")}
        result["nearest_resistance"] = {"error": sr_data.get("error")}

    # Pivot points
    if "error" not in pivot_data:
        result["pivot"] = {
            "P": pivot_data.get("P"),
            "R1": pivot_data.get("R1"),
            "S1": pivot_data.get("S1"),
            "R2": pivot_data.get("R2"),
            "S2": pivot_data.get("S2"),
        }
        result["position_vs_pivot"] = pivot_data.get("position")
    else:
        result["pivot"] = {"error": pivot_data.get("error")}

    # Distances (if we have a current price and S/R)
    if current_price is not None:
        supports_list = (
            [s["price"] for s in sr_data.get("supports", [])]
            if "error" not in sr_data else []
        )
        resistances_list = (
            [r["price"] for r in sr_data.get("resistances", [])]
            if "error" not in sr_data else []
        )
        if supports_list:
            nearest_sup = supports_list[0]
            result["dist_to_nearest_support_pts"] = round(current_price - nearest_sup, 3)
        if resistances_list:
            nearest_res = resistances_list[0]
            result["dist_to_nearest_resistance_pts"] = round(nearest_res - current_price, 3)

    return result


def _build_sl_tp_section(sl_tp_data: dict) -> dict:
    if "error" in sl_tp_data:
        return sl_tp_data
    return {
        "method": sl_tp_data.get("method", "atr"),
        "atr_1h": sl_tp_data.get("atr"),
        "sl": sl_tp_data.get("sl"),
        "tp": sl_tp_data.get("tp"),
        "risk_points": sl_tp_data.get("risk_points"),
        "reward_points": sl_tp_data.get("reward_points"),
        "rr_ratio": sl_tp_data.get("rr_ratio"),
    }


def _build_news_context(
    news_data: dict,
    events_data: dict,
    focus_hours: int,
) -> dict:
    now = datetime.now(tz=timezone.utc)
    result: dict = {f"digests_last_{focus_hours}h": 0}

    if "error" not in news_data:
        digests = news_data.get("digests", [])
        count = len(digests)
        result[f"digests_last_{focus_hours}h"] = count

        bullish = sum(
            1 for d in digests
            if (d.get("sentiment_label") or "").lower() == "bullish"
        )
        bearish = sum(
            1 for d in digests
            if (d.get("sentiment_label") or "").lower() == "bearish"
        )
        neutral = count - bullish - bearish

        scores_list = [
            d["sentiment_score"]
            for d in digests
            if d.get("sentiment_score") is not None
        ]
        avg_sentiment = round(sum(scores_list) / len(scores_list), 3) if scores_list else None

        result["bullish_count"] = bullish
        result["bearish_count"] = bearish
        result["neutral_count"] = neutral
        result["avg_sentiment"] = avg_sentiment

        # Top 5 headlines
        top = digests[:5]
        result["top_headlines"] = [
            {
                "timestamp": d.get("timestamp"),
                "summary": (d.get("summary") or "")[:200],
                "sentiment": d.get("sentiment_label"),
                "score": d.get("sentiment_score"),
            }
            for d in top
        ]
    else:
        result["error"] = news_data.get("error")

    # Upcoming events — flag those < 4h away
    upcoming: list[dict] = []
    big_event_imminent = False

    if "error" not in events_data:
        for ev in events_data.get("events", []):
            try:
                ev_dt_str = f"{ev['date']}T{ev['time_utc']}:00+00:00"
                ev_dt = datetime.fromisoformat(ev_dt_str)
                hours_away = (ev_dt - now).total_seconds() / 3600
                if hours_away < 0:
                    continue  # already past
                entry = {
                    "date": ev.get("date"),
                    "time_utc": ev.get("time_utc"),
                    "event": ev.get("event"),
                    "importance": ev.get("importance"),
                    "hours_away": round(hours_away, 1),
                }
                upcoming.append(entry)
                if hours_away < 4 and ev.get("importance") in ("HIGH", "high"):
                    big_event_imminent = True
            except Exception:
                continue

    result["upcoming_events"] = upcoming
    result["big_event_imminent"] = big_event_imminent

    return result


def _build_account_context(
    account_data: dict,
    campaigns_data: dict,
    side: str,
    current_price: float | None,
) -> dict:
    result: dict = {
        "proposed_layer_0_margin": 3000,
    }

    if "error" not in account_data:
        result["equity"] = account_data.get("equity")
        result["free_margin"] = account_data.get("free_margin")
    else:
        result["account_error"] = account_data.get("error")

    open_campaigns = campaigns_data.get("campaigns", []) if "error" not in campaigns_data else []
    result["open_campaigns"] = len(open_campaigns)

    conflicting = None
    same_side = None

    for camp in open_campaigns:
        camp_side = (camp.get("side") or "").upper()
        if side not in ("LONG", "SHORT"):
            # EITHER — just note what's open
            continue
        if camp_side and camp_side != side:
            conflicting = {
                "id": camp.get("id"),
                "side": camp_side,
                "status": camp.get("status"),
            }
        elif camp_side == side:
            same_side = {
                "id": camp.get("id"),
                "side": camp_side,
                "status": camp.get("status"),
                "layers": camp.get("layer_count") or camp.get("layers"),
            }

    result["conflicting_campaign"] = conflicting
    result["same_side_campaign"] = same_side

    # Proposed first layer sizing
    # DCA policy: layer 0 = $3000 margin, x10 leverage → $30k nominal
    # nominal / price = lots (contract size 100 barrels per lot)
    margin_0 = 3000
    nominal_0 = margin_0 * 10  # x10 leverage
    lots_0 = None
    if current_price and current_price > 0:
        lots_0 = round(nominal_0 / (current_price * 100), 3)  # contract = 100 bbl

    result["proposed_layer_0_margin"] = margin_0
    result["proposed_layer_0_lots"] = lots_0
    result["proposed_layer_0_nominal"] = nominal_0

    return result


def _build_correlation_section(corr_data: dict) -> dict:
    if "error" in corr_data:
        return {"error": corr_data.get("error")}
    r = corr_data.get("pearson_r")
    interp = corr_data.get("interpretation", "unknown")
    if r is not None and r < 0:
        note = "negative (normal): USD strength → Brent falls, USD weakness → Brent rises"
    elif r is not None:
        note = "positive (unusual): USD and Brent moving together"
    else:
        note = "unknown"
    return {
        "dxy_24h": r,
        "interpretation": interp,
        "note": note,
    }


# ---------------------------------------------------------------------------
# Verdict synthesis (pure heuristic — NO LLM call)
# ---------------------------------------------------------------------------

def _synthesize_verdict(
    side: str,
    current_price: float | None,
    scores_raw: dict,
    technical_levels: dict,
    suggested_sl_tp: dict,
    news_context: dict,
    account_context: dict,
    correlation: dict,
) -> dict:
    """Compute a conviction score and derive an action recommendation."""

    conviction = 0  # -100..+100
    pros: list[str] = []
    cons: list[str] = []

    uni_score = scores_raw.get("unified") if scores_raw else None
    fund_score = scores_raw.get("fundamental") if scores_raw else None
    tech_score = scores_raw.get("technical") if scores_raw else None
    sent_score_api = scores_raw.get("sentiment") if scores_raw else None

    is_long = side == "LONG"
    is_short = side == "SHORT"
    is_either = side == "EITHER"

    # --- 1. Score alignment ---
    if uni_score is not None:
        if is_long:
            if uni_score > 20:
                delta = min(int(uni_score / 5), 20)
                conviction += delta
                pros.append(f"Unified score +{uni_score:.1f} supports LONG thesis")
            elif uni_score < -20:
                delta = max(int(uni_score / 5), -20)
                conviction += delta
                cons.append(f"Unified score {uni_score:.1f} is bearish — counter-thesis for LONG")
        elif is_short:
            if uni_score < -20:
                delta = min(int(-uni_score / 5), 20)
                conviction += delta
                pros.append(f"Unified score {uni_score:.1f} supports SHORT thesis")
            elif uni_score > 20:
                delta = max(int(-uni_score / 5), -20)
                conviction += delta
                cons.append(f"Unified score +{uni_score:.1f} is bullish — counter-thesis for SHORT")

    # Fundamental score weight (strong signal)
    if fund_score is not None:
        if is_long and fund_score < -40:
            conviction -= 15
            cons.append(f"Fundamental score {fund_score:.1f} is strongly bearish")
        elif is_long and fund_score > 20:
            conviction += 8
            pros.append(f"Fundamental score +{fund_score:.1f} is supportive for LONG")
        elif is_short and fund_score > 40:
            conviction -= 15
            cons.append(f"Fundamental score +{fund_score:.1f} is strongly bullish — counter SHORT")
        elif is_short and fund_score < -20:
            conviction += 8
            pros.append(f"Fundamental score {fund_score:.1f} supports SHORT thesis")

    # --- 2. Price vs VWAP ---
    vwap = technical_levels.get("vwap_24h")
    dist_vwap_pct = technical_levels.get("distance_from_vwap_pct")
    if vwap and current_price and dist_vwap_pct is not None:
        price_vs_vwap = technical_levels.get("price_vs_vwap", "")
        if abs(dist_vwap_pct) < 0.2:
            conviction += 5
            pros.append(f"Price near 24h VWAP ({vwap:.2f}) — fair-value entry zone")
        elif is_long and price_vs_vwap == "below":
            conviction += 8
            pros.append(f"Price {abs(dist_vwap_pct):.2f}% below VWAP — discounted for LONG")
        elif is_short and price_vs_vwap == "above":
            conviction += 8
            pros.append(f"Price {abs(dist_vwap_pct):.2f}% above VWAP — stretched for SHORT")
        elif is_long and price_vs_vwap == "above" and dist_vwap_pct > 0.5:
            conviction -= 5
            cons.append(f"Price {dist_vwap_pct:.2f}% above VWAP — extended; chasing risk for LONG")

    # --- 3. S/R proximity ---
    dist_sup = technical_levels.get("dist_to_nearest_support_pts")
    dist_res = technical_levels.get("dist_to_nearest_resistance_pts")
    nearest_sup = technical_levels.get("nearest_support", [])
    nearest_res = technical_levels.get("nearest_resistance", [])

    if is_long and dist_sup is not None and current_price and dist_sup < (current_price * 0.003):
        conviction += 10
        pros.append(f"Price close to support ({nearest_sup[0] if nearest_sup else 'n/a'}) — good LONG foundation")
    if is_long and dist_res is not None and current_price and dist_res < (current_price * 0.003):
        conviction -= 8
        cons.append(f"Resistance just above ({nearest_res[0] if nearest_res else 'n/a'}) — limits upside for LONG")

    if is_short and dist_res is not None and current_price and dist_res < (current_price * 0.003):
        conviction += 10
        pros.append(f"Price close to resistance ({nearest_res[0] if nearest_res else 'n/a'}) — good SHORT foundation")
    if is_short and dist_sup is not None and current_price and dist_sup < (current_price * 0.003):
        conviction -= 8
        cons.append(f"Support just below ({nearest_sup[0] if nearest_sup else 'n/a'}) — limits downside for SHORT")

    # --- 4. News sentiment alignment ---
    avg_sent = news_context.get("avg_sentiment")
    bull_count = news_context.get("bullish_count", 0)
    bear_count = news_context.get("bearish_count", 0)
    total_digests_key = [k for k in news_context if k.startswith("digests_last_")]
    total_digests = news_context.get(total_digests_key[0], 0) if total_digests_key else 0

    if avg_sent is not None:
        if is_long and avg_sent > 0.1:
            conviction += 8
            pros.append(f"{bull_count}/{total_digests} recent digests bullish — sentiment supports LONG")
        elif is_long and avg_sent < -0.1:
            conviction -= 8
            cons.append(f"{bear_count}/{total_digests} recent digests bearish — sentiment opposes LONG")
        elif is_short and avg_sent < -0.1:
            conviction += 8
            pros.append(f"{bear_count}/{total_digests} recent digests bearish — sentiment supports SHORT")
        elif is_short and avg_sent > 0.1:
            conviction -= 8
            cons.append(f"{bull_count}/{total_digests} recent digests bullish — sentiment opposes SHORT")

    # --- 5. Upcoming event risk ---
    big_event = news_context.get("big_event_imminent", False)
    upcoming_events = news_context.get("upcoming_events", [])
    imminent_events = [e for e in upcoming_events if e.get("hours_away", 999) < 4]
    if big_event or imminent_events:
        conviction = int(conviction * 0.7)  # reduce by 30%
        ev_names = [e["event"] for e in imminent_events[:2]]
        cons.append(f"Major event(s) within 4h: {', '.join(ev_names) or 'unknown'} — binary risk")

    # --- 6. R:R check ---
    rr = suggested_sl_tp.get("rr_ratio")
    if rr is not None:
        if rr >= 2.0:
            conviction += 10
            pros.append(f"R:R ratio {rr:.2f} is excellent (≥ 2.0)")
        elif rr >= 1.5:
            conviction += 5
            pros.append(f"R:R ratio {rr:.2f} is acceptable (≥ 1.5)")
        else:
            conviction -= 15
            cons.append(f"R:R ratio {rr:.2f} is below 1.5 — poor risk/reward")

    # --- 7. DXY correlation ---
    dxy_r = correlation.get("dxy_24h")
    if dxy_r is not None and isinstance(dxy_r, (int, float)):
        if is_long and dxy_r < -0.5:
            pros.append(f"DXY corr {dxy_r:.2f} — USD weakness would lift Brent (LONG supportive)")
        elif is_short and dxy_r < -0.5:
            cons.append(f"DXY corr {dxy_r:.2f} — strong negative corr means USD strength needed for SHORT")

    # --- 8. Conflicting / same-side campaign ---
    conflicting = account_context.get("conflicting_campaign")
    same_side_camp = account_context.get("same_side_campaign")

    if conflicting:
        cons.append(
            f"Conflicting open campaign (ID {conflicting['id']}, {conflicting['side']}) — "
            f"would create opposite exposure"
        )
        # Force AVOID
        return {
            "action": "AVOID",
            "confidence": 0.0,
            "conviction_score": 0,
            "key_pros": pros,
            "key_cons": cons,
            "summary": (
                f"AVOID: You have an open {conflicting['side']} campaign. "
                f"Opening a {side} position would create conflicting exposure. "
                f"Close the existing campaign first."
            ),
        }

    if same_side_camp:
        pros.append(
            f"Existing {same_side_camp['side']} campaign open (ID {same_side_camp['id']}) — "
            f"can add DCA layer instead of opening new"
        )

    # --- Clamp conviction ---
    conviction = max(-100, min(100, conviction))

    # --- Determine action ---
    if big_event or imminent_events:
        action = "WAIT"
        summary_reason = "High-impact event imminent — wait for the dust to settle."
    elif is_long and conviction > 30:
        action = "ENTER_LONG"
        summary_reason = f"Conviction score +{conviction} supports a LONG entry."
    elif is_short and conviction > 30:
        action = "ENTER_SHORT"
        summary_reason = f"Conviction score +{conviction} supports a SHORT entry."
    elif abs(conviction) < 20:
        action = "WAIT"
        summary_reason = f"Conviction score {conviction:+d} is indecisive — no clear edge."
    elif is_either and abs(conviction) < 30:
        action = "WAIT"
        summary_reason = "No side specified and conviction is insufficient for entry."
    else:
        action = "WAIT"
        summary_reason = f"Mixed signals (conviction {conviction:+d}) — wait for clearer setup."

    # Confidence 0..1 derived from conviction
    confidence = round(min(abs(conviction) / 100, 1.0), 2)

    # One-sentence summary
    direction_word = side if side != "EITHER" else "either direction"
    summary = f"{action} — {summary_reason}"
    if action in ("ENTER_LONG", "ENTER_SHORT") and current_price:
        sl = suggested_sl_tp.get("sl")
        tp = suggested_sl_tp.get("tp")
        if sl and tp:
            summary += f" Entry ~{current_price:.2f}, SL {sl:.2f}, TP {tp:.2f}."

    return {
        "action": action,
        "confidence": confidence,
        "conviction_score": conviction,
        "key_pros": pros,
        "key_cons": cons,
        "summary": summary,
    }
