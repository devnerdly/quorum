"""Tests for the Telegram alert formatter."""

from __future__ import annotations

import pytest

from formatter import format_signal_alert, format_system_alert, ACTION_EMOJI


SAMPLE_SHORT = {
    "timestamp": "2026-04-02T10:00:00+00:00",
    "action": "SHORT",
    "unified_score": -72.5,
    "confidence": 0.88,
    "entry_price": 85.30,
    "stop_loss": 87.50,
    "take_profit": 80.00,
    "haiku_summary": "Oil falls like rain\nMarket bears growl with triumph\nSell the fleeting peak",
    "grok_narrative": "Bearish momentum dominates Brent crude as inventories swell.",
    "opus_analysis": "Strong short signal confirmed by technical and fundamental confluence.",
    "risk_factors": ["Geopolitical flare-up", "Unexpected OPEC cut"],
}

SAMPLE_LONG = {
    "timestamp": "2026-04-02T12:00:00+00:00",
    "action": "LONG",
    "unified_score": 65.0,
    "confidence": 0.75,
    "entry_price": 82.10,
    "stop_loss": 79.50,
    "take_profit": 88.00,
    "haiku_summary": None,
    "grok_narrative": None,
    "opus_analysis": None,
    "risk_factors": [],
}

SAMPLE_WAIT = {
    "timestamp": "2026-04-02T14:00:00+00:00",
    "action": "WAIT",
    "unified_score": 5.0,
    "confidence": 0.40,
    "entry_price": None,
    "stop_loss": None,
    "take_profit": None,
    "haiku_summary": None,
    "grok_narrative": None,
    "opus_analysis": None,
    "risk_factors": None,
}


class TestFormatSignalAlert:
    def test_short_alert_contains_action(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "SHORT" in text

    def test_short_alert_contains_score(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "-72.50" in text

    def test_short_alert_contains_confidence(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "88%" in text

    def test_short_alert_contains_entry_price(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "85.30" in text

    def test_short_alert_contains_stop_loss(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "87.50" in text

    def test_short_alert_contains_take_profit(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "80.00" in text

    def test_short_alert_contains_haiku(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "Oil falls like rain" in text

    def test_short_alert_contains_narrative(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "inventories swell" in text

    def test_short_alert_contains_opus(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "confluence" in text

    def test_short_alert_contains_risk_factors(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert "Geopolitical flare-up" in text

    def test_short_alert_uses_red_emoji(self):
        text = format_signal_alert(SAMPLE_SHORT)
        assert ACTION_EMOJI["SHORT"] in text

    def test_long_alert_contains_action(self):
        text = format_signal_alert(SAMPLE_LONG)
        assert "LONG" in text

    def test_long_alert_uses_green_emoji(self):
        text = format_signal_alert(SAMPLE_LONG)
        assert ACTION_EMOJI["LONG"] in text

    def test_long_alert_contains_entry_price(self):
        text = format_signal_alert(SAMPLE_LONG)
        assert "82.10" in text

    def test_long_alert_handles_missing_optional_fields(self):
        """Optional fields absent should not raise errors."""
        text = format_signal_alert(SAMPLE_LONG)
        assert "LONG" in text

    def test_wait_alert_contains_action(self):
        text = format_signal_alert(SAMPLE_WAIT)
        assert "WAIT" in text

    def test_wait_alert_uses_yellow_emoji(self):
        text = format_signal_alert(SAMPLE_WAIT)
        assert ACTION_EMOJI["WAIT"] in text

    def test_wait_alert_shows_na_for_missing_prices(self):
        text = format_signal_alert(SAMPLE_WAIT)
        assert "N/A" in text

    def test_unknown_action_falls_back_to_yellow_emoji(self):
        rec = {**SAMPLE_WAIT, "action": "UNKNOWN"}
        text = format_signal_alert(rec)
        assert ACTION_EMOJI["WAIT"] in text  # fallback is yellow circle


class TestFormatSystemAlert:
    def test_contains_message(self):
        text = format_system_alert("Redis connection lost")
        assert "Redis connection lost" in text

    def test_contains_warning_emoji(self):
        text = format_system_alert("test")
        assert "\u26a0" in text

    def test_contains_system_alert_label(self):
        text = format_system_alert("test")
        assert "System Alert" in text
