"""Tests for the RSS sentiment classifier."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message(text: str) -> MagicMock:
    """Build a mock anthropic Messages response with a single content block."""
    content_block = MagicMock()
    content_block.text = text
    message = MagicMock()
    message.content = [content_block]
    return message


# ---------------------------------------------------------------------------
# Tests for classify_article
# ---------------------------------------------------------------------------


class TestClassifyArticle:
    """Unit tests for classify_article(), with the Anthropic client mocked."""

    def _run(self, mock_create, title: str = "Oil prices rise", source: str = "reuters"):
        """Import classify_article and call it; mock is already in place."""
        from services.sentiment.sources.rss import classify_article  # type: ignore[import]

        return classify_article(title, source)

    # ------------------------------------------------------------------
    # Happy-path: bullish classification
    # ------------------------------------------------------------------

    def test_bullish_classification(self):
        response_json = '{"sentiment": "bullish", "score": 0.75, "relevance": 0.9}'
        mock_message = _make_message(response_json)

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_message

            # Reload module inside patch context so settings are irrelevant
            import importlib
            import sys

            # Ensure the module is freshly imported within the patch
            mod_name = "services.sentiment.sources.rss"
            if mod_name in sys.modules:
                del sys.modules[mod_name]

            from services.sentiment.sources.rss import classify_article  # type: ignore[import]

            result = classify_article("OPEC cuts boost oil prices", "reuters")

        assert result["sentiment"] == "bullish"
        assert result["score"] == pytest.approx(0.75)
        assert result["relevance"] == pytest.approx(0.9)

    # ------------------------------------------------------------------
    # Happy-path: bearish classification
    # ------------------------------------------------------------------

    def test_bearish_classification(self):
        response_json = '{"sentiment": "bearish", "score": -0.5, "relevance": 0.8}'
        mock_message = _make_message(response_json)

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_message

            import sys
            mod_name = "services.sentiment.sources.rss"
            if mod_name in sys.modules:
                del sys.modules[mod_name]

            from services.sentiment.sources.rss import classify_article  # type: ignore[import]

            result = classify_article("Recession fears hammer crude oil", "oilprice")

        assert result["sentiment"] == "bearish"
        assert result["score"] == pytest.approx(-0.5)
        assert result["relevance"] == pytest.approx(0.8)

    # ------------------------------------------------------------------
    # Happy-path: neutral classification
    # ------------------------------------------------------------------

    def test_neutral_classification(self):
        response_json = '{"sentiment": "neutral", "score": 0.0, "relevance": 0.4}'
        mock_message = _make_message(response_json)

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_message

            import sys
            mod_name = "services.sentiment.sources.rss"
            if mod_name in sys.modules:
                del sys.modules[mod_name]

            from services.sentiment.sources.rss import classify_article  # type: ignore[import]

            result = classify_article("Markets await Fed decision", "reuters")

        assert result["sentiment"] == "neutral"
        assert result["score"] == pytest.approx(0.0)
        assert result["relevance"] == pytest.approx(0.4)

    # ------------------------------------------------------------------
    # Edge case: API raises exception → fallback to neutral
    # ------------------------------------------------------------------

    def test_api_error_returns_fallback(self):
        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.side_effect = RuntimeError("network error")

            import sys
            mod_name = "services.sentiment.sources.rss"
            if mod_name in sys.modules:
                del sys.modules[mod_name]

            from services.sentiment.sources.rss import classify_article  # type: ignore[import]

            result = classify_article("Some headline", "reuters")

        assert result == {"sentiment": "neutral", "score": 0.0, "relevance": 0.0}

    # ------------------------------------------------------------------
    # Edge case: malformed JSON from API → fallback to neutral
    # ------------------------------------------------------------------

    def test_malformed_json_returns_fallback(self):
        mock_message = _make_message("not valid json {{{{")

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_message

            import sys
            mod_name = "services.sentiment.sources.rss"
            if mod_name in sys.modules:
                del sys.modules[mod_name]

            from services.sentiment.sources.rss import classify_article  # type: ignore[import]

            result = classify_article("Some headline", "reuters")

        assert result == {"sentiment": "neutral", "score": 0.0, "relevance": 0.0}

    # ------------------------------------------------------------------
    # Verify the correct Haiku model is used
    # ------------------------------------------------------------------

    def test_uses_haiku_model(self):
        response_json = '{"sentiment": "neutral", "score": 0.0, "relevance": 0.5}'
        mock_message = _make_message(response_json)

        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.messages.create.return_value = mock_message

            import sys
            mod_name = "services.sentiment.sources.rss"
            if mod_name in sys.modules:
                del sys.modules[mod_name]

            from services.sentiment.sources.rss import classify_article, _HAIKU_MODEL  # type: ignore[import]

            classify_article("Oil steady", "reuters")

            call_kwargs = mock_client.messages.create.call_args
            assert call_kwargs.kwargs.get("model") == _HAIKU_MODEL or call_kwargs.args[0] == _HAIKU_MODEL or "model" in str(call_kwargs)
            # Check via keyword args dict
            assert mock_client.messages.create.call_args.kwargs["model"] == _HAIKU_MODEL
