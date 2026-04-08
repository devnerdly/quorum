"""Alert formatter for Telegram notifications."""

from __future__ import annotations

ACTION_EMOJI = {
    "BUY": "\U0001f7e2",    # green circle
    "LONG": "\U0001f7e2",   # green circle
    "SELL": "\U0001f534",   # red circle
    "SHORT": "\U0001f534",  # red circle
    "HOLD": "\U0001f7e1",   # yellow circle
    "WAIT": "\U0001f7e1",   # yellow circle
}

WARNING_EMOJI = "\u26a0\ufe0f"


def format_signal_alert(rec: dict) -> str:
    """Format a recommendation dict into a Telegram alert message.

    Parameters
    ----------
    rec:
        Dictionary with RecommendationEvent fields.

    Returns
    -------
    str
        Formatted Telegram message (plain text / Markdown-friendly).
    """
    action = str(rec.get("action", "WAIT")).upper()
    emoji = ACTION_EMOJI.get(action, "\U0001f7e1")

    score = rec.get("unified_score")
    if score is None:
        score = rec.get("opus_override_score")
    score_str = f"{score:+.0f}/100" if score is not None else "N/A"

    confidence = rec.get("confidence")
    confidence_str = f"{confidence:.0%}" if confidence is not None else "N/A"

    entry = rec.get("entry_price")
    sl = rec.get("stop_loss")
    tp = rec.get("take_profit")
    entry_str = f"${entry:.2f}" if entry is not None else "N/A"
    sl_str = f"${sl:.2f}" if sl is not None else "N/A"
    tp_str = f"${tp:.2f}" if tp is not None else "N/A"

    haiku = rec.get("haiku_summary") or ""
    narrative = rec.get("grok_narrative") or ""
    opus = rec.get("opus_analysis") or ""

    risk_factors = rec.get("risk_factors") or []
    if isinstance(risk_factors, list) and risk_factors:
        risk_lines = "\n".join(f"  - {r}" for r in risk_factors)
    elif isinstance(risk_factors, str) and risk_factors:
        risk_lines = f"  - {risk_factors}"
    else:
        risk_lines = "  N/A"

    timestamp = rec.get("timestamp", "")

    lines = [
        f"{emoji} *Brent Crude Signal: {action}*",
        f"Score: {score_str} | Confidence: {confidence_str}",
        "",
    ]

    if haiku:
        lines += [
            "*Haiku Summary*",
            haiku,
            "",
        ]

    if narrative:
        lines += [
            "*Market Narrative*",
            narrative,
            "",
        ]

    if opus:
        lines += [
            "*Opus Analysis*",
            opus,
            "",
        ]

    lines += [
        "*Trade Levels*",
        f"Entry:     {entry_str}",
        f"Stop-Loss: {sl_str}",
        f"Take-Profit: {tp_str}",
        "",
        "*Risk Factors*",
        risk_lines,
    ]

    if timestamp:
        lines += ["", f"_Generated: {timestamp}_"]

    return "\n".join(lines)


def format_system_alert(message: str) -> str:
    """Format a system/operational alert."""
    return f"{WARNING_EMOJI} *System Alert*\n{message}"


_POSITION_EVENT_TITLES = {
    "opened":          ("\U0001f4e5", "Position OPENED"),    # inbox tray
    "tp_hit":          ("\U0001f3af", "TAKE-PROFIT HIT"),    # bullseye
    "sl_hit":          ("\U0001f6d1", "STOP-LOSS HIT"),      # stop sign
    "strategy_close":  ("\U0001f504", "Position CLOSED by strategy"),  # arrows in cycle
    "manual_close":    ("\u270b", "Position CLOSED manually"),         # hand
}


def format_marketfeed_digest(evt: dict) -> str | None:
    """Format a 5-minute @marketfeed knowledge digest."""
    if str(evt.get("type", "")) != "marketfeed_digest":
        return None

    sentiment_label = str(evt.get("sentiment_label", "neutral")).lower()
    score = evt.get("sentiment_score") or 0.0
    icon = (
        "\U0001f7e2" if sentiment_label == "bullish"
        else "\U0001f534" if sentiment_label == "bearish"
        else "\U0001f7e1"
    )

    count = evt.get("message_count", 0)
    window = evt.get("window", "5min")

    lines = [
        f"{icon} *@marketfeed digest* ({window}, {count} msgs)",
        f"Sentiment: {sentiment_label.upper()} ({score:+.2f})",
        "",
    ]

    summary = (evt.get("summary") or "").strip()
    if summary:
        lines += ["*Summary*", summary, ""]

    key_events = evt.get("key_events") or []
    if isinstance(key_events, list) and key_events:
        lines.append("*Key Events*")
        for ev in key_events[:6]:
            lines.append(f"  • {ev}")
        lines.append("")

    ts = evt.get("timestamp")
    if ts:
        lines.append(f"_at {ts}_")

    return "\n".join(lines)


def format_position_event(evt: dict) -> str | None:
    """Format a Position lifecycle event into a Telegram alert."""
    kind = str(evt.get("type", "")).lower()
    if kind not in _POSITION_EVENT_TITLES:
        return None

    icon, title = _POSITION_EVENT_TITLES[kind]
    side = str(evt.get("side", "")).upper()
    pos_id = evt.get("id")

    lines = [
        f"{icon} *{title}*",
        f"Position #{pos_id} — {side}",
        "",
    ]

    entry = evt.get("entry_price")
    close_p = evt.get("close_price")
    sl = evt.get("stop_loss")
    tp = evt.get("take_profit")
    pnl = evt.get("realised_pnl")

    if entry is not None:
        lines.append(f"Entry:       ${entry:.2f}")
    if sl is not None and close_p is None:
        lines.append(f"Stop-Loss:   ${sl:.2f}")
    if tp is not None and close_p is None:
        lines.append(f"Take-Profit: ${tp:.2f}")
    if close_p is not None:
        lines.append(f"Close:       ${close_p:.2f}")
    if pnl is not None:
        sign = "+" if pnl >= 0 else ""
        lines.append(f"P/L:         {sign}${pnl:.2f}")

    notes = evt.get("notes") or evt.get("reason")
    if notes:
        lines += ["", f"_{notes}_"]

    ts = evt.get("timestamp")
    if ts:
        lines += ["", f"_at {ts}_"]

    return "\n".join(lines)
