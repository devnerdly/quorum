"""Tests for the Grok get_twitter_narrative agent."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from agents.grok import get_twitter_narrative, FALLBACK, MODEL


class TestGetTwitterNarrative:
    def test_returns_model_text_on_success(self):
        """get_twitter_narrative should return the model's message content."""
        expected = "Twitter is buzzing about OPEC cuts pushing Brent higher."

        mock_message = MagicMock()
        mock_message.content = expected

        mock_choice = MagicMock()
        mock_choice.message = mock_message

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        with patch("agents.grok.OpenAI") as MockClient:
            instance = MockClient.return_value
            instance.chat.completions.create.return_value = mock_response

            result = get_twitter_narrative()

        assert result == expected.strip()

    def test_calls_grok_model(self):
        """get_twitter_narrative should call the grok-3 model."""
        mock_message = MagicMock()
        mock_message.content = "Narrative."

        mock_choice = MagicMock()
        mock_choice.message = mock_message

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        with patch("agents.grok.OpenAI") as MockClient:
            instance = MockClient.return_value
            instance.chat.completions.create.return_value = mock_response

            get_twitter_narrative()

            call_kwargs = instance.chat.completions.create.call_args
            kwargs = call_kwargs.kwargs if call_kwargs.kwargs else call_kwargs[1]
            assert kwargs.get("model") == MODEL

    def test_uses_xai_base_url(self):
        """OpenAI client should be initialised with the xAI base URL."""
        mock_message = MagicMock()
        mock_message.content = "Narrative."

        mock_choice = MagicMock()
        mock_choice.message = mock_message

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        with patch("agents.grok.OpenAI") as MockClient:
            instance = MockClient.return_value
            instance.chat.completions.create.return_value = mock_response

            get_twitter_narrative()

            init_kwargs = MockClient.call_args.kwargs if MockClient.call_args.kwargs else MockClient.call_args[1]
            assert init_kwargs.get("base_url") == "https://api.x.ai/v1"

    def test_respects_max_tokens(self):
        """get_twitter_narrative should request at most 200 tokens."""
        mock_message = MagicMock()
        mock_message.content = "Narrative."

        mock_choice = MagicMock()
        mock_choice.message = mock_message

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        with patch("agents.grok.OpenAI") as MockClient:
            instance = MockClient.return_value
            instance.chat.completions.create.return_value = mock_response

            get_twitter_narrative()

            call_kwargs = instance.chat.completions.create.call_args
            kwargs = call_kwargs.kwargs if call_kwargs.kwargs else call_kwargs[1]
            assert kwargs.get("max_tokens", 0) <= 200

    def test_returns_fallback_on_api_error(self):
        """get_twitter_narrative should return the fallback string on any exception."""
        with patch("agents.grok.OpenAI") as MockClient:
            instance = MockClient.return_value
            instance.chat.completions.create.side_effect = RuntimeError("xAI error")

            result = get_twitter_narrative()

        assert result == FALLBACK

    def test_strips_whitespace_from_response(self):
        """get_twitter_narrative should strip whitespace from the model text."""
        mock_message = MagicMock()
        mock_message.content = "  Market is bullish.  \n"

        mock_choice = MagicMock()
        mock_choice.message = mock_message

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        with patch("agents.grok.OpenAI") as MockClient:
            instance = MockClient.return_value
            instance.chat.completions.create.return_value = mock_response

            result = get_twitter_narrative()

        assert not result.startswith(" ")
        assert not result.endswith("\n")
