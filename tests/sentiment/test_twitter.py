"""Tests for the Twitter/X Grok sentiment parser."""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# We import parse_grok_response directly — no network or DB calls needed.
# ---------------------------------------------------------------------------


def _import_parse():
    """Import parse_grok_response, reloading the module to avoid cache issues."""
    import sys
    mod_name = "services.sentiment.sources.twitter"
    if mod_name in sys.modules:
        # Use cached version — it's a pure function, no side effects on import
        pass
    from services.sentiment.sources.twitter import parse_grok_response  # type: ignore[import]
    return parse_grok_response


class TestParseGrokResponse:
    """Unit tests for parse_grok_response()."""

    # ------------------------------------------------------------------
    # Happy-path: valid JSON with all fields
    # ------------------------------------------------------------------

    def test_bullish_response(self):
        parse = _import_parse()
        text = '{"score": 0.6, "narrative": "OPEC supply cut optimism", "topics": ["#OPEC", "#CrudeOil"]}'
        result = parse(text)

        assert result["score"] == pytest.approx(0.6)
        assert result["narrative"] == "OPEC supply cut optimism"
        assert "#OPEC" in result["topics"]
        assert "#CrudeOil" in result["topics"]

    def test_bearish_response(self):
        parse = _import_parse()
        text = '{"score": -0.7, "narrative": "Recession demand fears", "topics": ["#recession", "demand"]}'
        result = parse(text)

        assert result["score"] == pytest.approx(-0.7)
        assert result["narrative"] == "Recession demand fears"
        assert "recession" in result["topics"].lower()

    def test_neutral_response(self):
        parse = _import_parse()
        text = '{"score": 0.0, "narrative": "Mixed signals from OPEC meeting", "topics": ["#OPEC", "#Brent", "neutral"]}'
        result = parse(text)

        assert result["score"] == pytest.approx(0.0)
        assert "OPEC" in result["narrative"]

    # ------------------------------------------------------------------
    # Topics as comma-joined string
    # ------------------------------------------------------------------

    def test_topics_joined_as_string(self):
        parse = _import_parse()
        text = '{"score": 0.2, "narrative": "Mild optimism", "topics": ["#Oil", "#Brent", "supply"]}'
        result = parse(text)

        assert isinstance(result["topics"], str)
        assert "#Oil" in result["topics"]
        assert "#Brent" in result["topics"]
        assert "supply" in result["topics"]

    # ------------------------------------------------------------------
    # Edge case: topics is an empty list
    # ------------------------------------------------------------------

    def test_empty_topics_list(self):
        parse = _import_parse()
        text = '{"score": 0.1, "narrative": "Quiet day", "topics": []}'
        result = parse(text)

        assert result["topics"] == ""

    # ------------------------------------------------------------------
    # Edge case: markdown code fence wrapping
    # ------------------------------------------------------------------

    def test_markdown_code_fence(self):
        parse = _import_parse()
        text = '```json\n{"score": 0.4, "narrative": "Bulls dominate", "topics": ["#bullish"]}\n```'
        result = parse(text)

        assert result["score"] == pytest.approx(0.4)
        assert result["narrative"] == "Bulls dominate"

    # ------------------------------------------------------------------
    # Edge case: missing keys → sensible defaults
    # ------------------------------------------------------------------

    def test_missing_score_defaults_to_zero(self):
        parse = _import_parse()
        text = '{"narrative": "Some narrative", "topics": ["#oil"]}'
        result = parse(text)

        assert result["score"] == pytest.approx(0.0)

    def test_missing_narrative_defaults(self):
        parse = _import_parse()
        text = '{"score": 0.3, "topics": ["#oil"]}'
        result = parse(text)

        assert result["narrative"] == "unknown"

    # ------------------------------------------------------------------
    # Edge case: invalid JSON → fallback
    # ------------------------------------------------------------------

    def test_invalid_json_returns_fallback(self):
        parse = _import_parse()
        result = parse("this is not json at all")

        assert result == {"score": 0.0, "narrative": "parse error", "topics": ""}

    # ------------------------------------------------------------------
    # Edge case: empty string → fallback
    # ------------------------------------------------------------------

    def test_empty_string_returns_fallback(self):
        parse = _import_parse()
        result = parse("")

        assert result == {"score": 0.0, "narrative": "parse error", "topics": ""}

    # ------------------------------------------------------------------
    # Score clamping not enforced by parser (Grok should be trusted),
    # but verify float conversion works for boundary values
    # ------------------------------------------------------------------

    def test_score_boundary_positive_one(self):
        parse = _import_parse()
        text = '{"score": 1.0, "narrative": "Extremely bullish", "topics": []}'
        result = parse(text)

        assert result["score"] == pytest.approx(1.0)

    def test_score_boundary_negative_one(self):
        parse = _import_parse()
        text = '{"score": -1.0, "narrative": "Extremely bearish", "topics": []}'
        result = parse(text)

        assert result["score"] == pytest.approx(-1.0)
