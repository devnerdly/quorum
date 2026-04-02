"""Alert formatter for Telegram notifications."""

from __future__ import annotations

ACTION_EMOJI = {
    "LONG": "\U0001f7e2",   # green circle
    "SHORT": "\U0001f534",  # red circle
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

    score = rec.get("unified_score") or rec.get("opus_override_score")
    score_str = f"{score:.2f}" if score is not None else "N/A"

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
