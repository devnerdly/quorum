"""Tests for the Haiku summarize_scores agent."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from agents.haiku import summarize_scores, FALLBACK, MODEL


SAMPLE_SCORES = {
    "technical_score": 0.35,
    "fundamental_score": -0.10,
    "sentiment_score": 0.20,
    "shipping_score": None,
    "unified_score": 0.15,
}


class TestSummarizeScores:
    def test_returns_model_text_on_success(self):
        """summarize_scores should return the model's text content."""
        expected = "Brent crude is showing mild bullish momentum."

        mock_content = MagicMock()
        mock_content.text = expected

        mock_response = MagicMock()
        mock_response.content = [mock_content]

        with patch("agents.haiku.anthropic.Anthropic") as MockClient:
            instance = MockClient.return_value
            instance.messages.create.return_value = mock_response

            result = summarize_scores(SAMPLE_SCORES)

        assert result == expected

    def test_calls_correct_model(self):
        """summarize_scores should call the Haiku model."""
        mock_content = MagicMock()
        mock_content.text = "Summary text."

        mock_response = MagicMock()
        mock_response.content = [mock_content]

        with patch("agents.haiku.anthropic.Anthropic") as MockClient:
            instance = MockClient.return_value
            instance.messages.create.return_value = mock_response

            summarize_scores(SAMPLE_SCORES)

            call_kwargs = instance.messages.create.call_args
            assert call_kwargs.kwargs.get("model") == MODEL or call_kwargs[1].get("model") == MODEL or MODEL in str(call_kwargs)

    def test_respects_max_tokens(self):
        """summarize_scores should request at most 300 tokens."""
        mock_content = MagicMock()
        mock_content.text = "Summary."

        mock_response = MagicMock()
        mock_response.content = [mock_content]

        with patch("agents.haiku.anthropic.Anthropic") as MockClient:
            instance = MockClient.return_value
            instance.messages.create.return_value = mock_response

            summarize_scores(SAMPLE_SCORES)

            call_kwargs = instance.messages.create.call_args
            # Accept both positional and keyword argument forms
            kwargs = call_kwargs.kwargs if call_kwargs.kwargs else call_kwargs[1]
            assert kwargs.get("max_tokens", 0) <= 300

    def test_returns_fallback_on_api_error(self):
        """summarize_scores should return the fallback string when the API raises."""
        with patch("agents.haiku.anthropic.Anthropic") as MockClient:
            instance = MockClient.return_value
            instance.messages.create.side_effect = RuntimeError("API error")

            result = summarize_scores(SAMPLE_SCORES)

        assert result == FALLBACK

    def test_strips_whitespace_from_response(self):
        """summarize_scores should strip leading/trailing whitespace."""
        mock_content = MagicMock()
        mock_content.text = "  Summary with spaces.  \n"

        mock_response = MagicMock()
        mock_response.content = [mock_content]

        with patch("agents.haiku.anthropic.Anthropic") as MockClient:
            instance = MockClient.return_value
            instance.messages.create.return_value = mock_response

            result = summarize_scores(SAMPLE_SCORES)

        assert not result.startswith(" ")
        assert not result.endswith(" ")
        assert not result.endswith("\n")

    def test_passes_scores_in_prompt(self):
        """The prompt sent to the model should reference the scores."""
        mock_content = MagicMock()
        mock_content.text = "Summary."

        mock_response = MagicMock()
        mock_response.content = [mock_content]

        with patch("agents.haiku.anthropic.Anthropic") as MockClient:
            instance = MockClient.return_value
            instance.messages.create.return_value = mock_response

            summarize_scores(SAMPLE_SCORES)

            call_kwargs = instance.messages.create.call_args
            kwargs = call_kwargs.kwargs if call_kwargs.kwargs else call_kwargs[1]
            messages = kwargs.get("messages", [])
            user_content = " ".join(m.get("content", "") for m in messages)
            # At least one score key should appear in the prompt
            assert any(k in user_content for k in SAMPLE_SCORES)
