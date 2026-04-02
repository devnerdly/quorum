"""Tests for the Opus agent — focusing on parse_opus_response."""

from __future__ import annotations

import json

import pytest

from agents.opus import parse_opus_response


VALID_JSON = {
    "unified_score": 0.4,
    "opus_override_score": None,
    "confidence": 0.75,
    "action": "BUY",
    "analysis_text": "Bullish breakout confirmed.",
    "base_scenario": "Price moves to $85 within 24h.",
    "alt_scenario": "Pullback to $80 if inventory data disappoints.",
    "risk_factors": ["OPEC surprise", "USD strength"],
    "entry_price": 83.5,
    "stop_loss": 81.0,
    "take_profit": 87.0,
}


class TestParseOpusResponse:
    def test_parses_plain_json(self):
        """parse_opus_response should parse a plain JSON string."""
        text = json.dumps(VALID_JSON)
        result = parse_opus_response(text)
        assert result["action"] == "BUY"
        assert result["confidence"] == 0.75

    def test_strips_json_code_fence(self):
        """parse_opus_response should handle ```json ... ``` fences."""
        text = f"```json\n{json.dumps(VALID_JSON)}\n```"
        result = parse_opus_response(text)
        assert result["action"] == "BUY"

    def test_strips_plain_code_fence(self):
        """parse_opus_response should handle ``` ... ``` fences without language tag."""
        text = f"```\n{json.dumps(VALID_JSON)}\n```"
        result = parse_opus_response(text)
        assert result["action"] == "BUY"

    def test_extracts_json_from_surrounding_text(self):
        """parse_opus_response should extract JSON even if surrounded by prose."""
        text = f"Here is my recommendation:\n{json.dumps(VALID_JSON)}\nThank you."
        result = parse_opus_response(text)
        assert result["action"] == "BUY"

    def test_raises_on_invalid_json(self):
        """parse_opus_response should raise an exception for unparseable input."""
        with pytest.raises(Exception):
            parse_opus_response("This is not JSON at all.")

    def test_preserves_all_fields(self):
        """All fields from the JSON should be present in the parsed dict."""
        text = json.dumps(VALID_JSON)
        result = parse_opus_response(text)
        for key in VALID_JSON:
            assert key in result

    def test_handles_null_values(self):
        """parse_opus_response should correctly parse null (None) JSON values."""
        rec = dict(VALID_JSON)
        rec["opus_override_score"] = None
        rec["entry_price"] = None
        text = json.dumps(rec)
        result = parse_opus_response(text)
        assert result["opus_override_score"] is None
        assert result["entry_price"] is None

    def test_handles_nested_list(self):
        """risk_factors as a list should round-trip correctly."""
        text = json.dumps(VALID_JSON)
        result = parse_opus_response(text)
        assert isinstance(result["risk_factors"], list)
        assert "OPEC surprise" in result["risk_factors"]

    def test_handles_whitespace_around_fence(self):
        """Code fences with extra whitespace should still parse correctly."""
        text = f"\n  ```json\n  {json.dumps(VALID_JSON)}\n  ```\n"
        result = parse_opus_response(text)
        assert result["action"] == "BUY"

    def test_sell_action(self):
        """parse_opus_response should work for SELL recommendations."""
        rec = dict(VALID_JSON)
        rec["action"] = "SELL"
        rec["unified_score"] = -0.6
        text = json.dumps(rec)
        result = parse_opus_response(text)
        assert result["action"] == "SELL"
        assert result["unified_score"] == pytest.approx(-0.6)

    def test_hold_action(self):
        """parse_opus_response should work for HOLD recommendations."""
        rec = dict(VALID_JSON)
        rec["action"] = "HOLD"
        rec["confidence"] = 0.5
        text = json.dumps(rec)
        result = parse_opus_response(text)
        assert result["action"] == "HOLD"

    def test_empty_string_raises(self):
        """parse_opus_response should raise on empty string input."""
        with pytest.raises(Exception):
            parse_opus_response("")
