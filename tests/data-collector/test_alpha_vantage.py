"""Tests for the Alpha Vantage collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _av_response(series_key: str = "Time Series (5min)") -> dict:
    """Minimal Alpha Vantage JSON response for intraday data."""
    return {
        "Meta Data": {
            "1. Information": "Intraday (5min) open, high, low, close prices and volume",
            "2. Symbol": "BZ",
        },
        series_key: {
            "2024-01-02 10:05:00": {
                "1. open": "77.50",
                "2. high": "78.10",
                "3. low": "77.30",
                "4. close": "77.90",
                "5. volume": "5000",
            },
            "2024-01-02 10:00:00": {
                "1. open": "77.10",
                "2. high": "77.60",
                "3. low": "76.80",
                "4. close": "77.50",
                "5. volume": "4200",
            },
        },
    }


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


# ---------------------------------------------------------------------------
# fetch_brent_ohlcv_av
# ---------------------------------------------------------------------------

class TestFetchBrentOhlcvAv:
    def test_returns_correct_number_of_records(self):
        with patch("requests.get", return_value=_mock_response(_av_response())):
            from collectors.alpha_vantage import fetch_brent_ohlcv_av

            records = fetch_brent_ohlcv_av(interval="5min")

        assert len(records) == 2

    def test_record_fields_and_types(self):
        with patch("requests.get", return_value=_mock_response(_av_response())):
            from collectors.alpha_vantage import fetch_brent_ohlcv_av

            records = fetch_brent_ohlcv_av(interval="5min")

        rec = records[0]  # earliest after sort
        assert set(rec.keys()) == {"timestamp", "source", "timeframe", "open", "high", "low", "close", "volume"}
        assert rec["source"] == "alpha_vantage"
        assert rec["timeframe"] == "5min"
        assert isinstance(rec["timestamp"], datetime)
        assert rec["timestamp"].tzinfo is not None
        assert rec["open"] == pytest.approx(77.10)
        assert rec["volume"] == pytest.approx(4200.0)

    def test_records_sorted_ascending(self):
        with patch("requests.get", return_value=_mock_response(_av_response())):
            from collectors.alpha_vantage import fetch_brent_ohlcv_av

            records = fetch_brent_ohlcv_av(interval="5min")

        # Earliest timestamp first
        assert records[0]["timestamp"] < records[1]["timestamp"]

    def test_empty_series_returns_empty_list(self):
        empty_resp = {"Meta Data": {}, "Time Series (5min)": {}}
        with patch("requests.get", return_value=_mock_response(empty_resp)):
            from collectors.alpha_vantage import fetch_brent_ohlcv_av

            result = fetch_brent_ohlcv_av(interval="5min")

        assert result == []

    def test_missing_series_key_returns_empty_list(self):
        resp = {"Meta Data": {}, "Note": "API rate limit exceeded"}
        with patch("requests.get", return_value=_mock_response(resp)):
            from collectors.alpha_vantage import fetch_brent_ohlcv_av

            result = fetch_brent_ohlcv_av(interval="5min")

        assert result == []

    def test_invalid_interval_raises(self):
        with pytest.raises(ValueError, match="Unsupported interval"):
            from collectors.alpha_vantage import fetch_brent_ohlcv_av

            # No HTTP call needed — should raise before the request
            fetch_brent_ohlcv_av(interval="99x")

    def test_requests_get_called_with_correct_params(self):
        with patch("requests.get", return_value=_mock_response(_av_response())) as mock_get:
            # Patch settings so no real API key needed
            with patch("collectors.alpha_vantage.settings") as mock_settings:
                mock_settings.alpha_vantage_api_key = "TESTKEY"

                from collectors.alpha_vantage import fetch_brent_ohlcv_av

                fetch_brent_ohlcv_av(interval="5min")

        call_kwargs = mock_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs.args[1]
        assert params["function"] == "TIME_SERIES_INTRADAY"
        assert params["symbol"] == "BZ"
        assert params["interval"] == "5min"


# ---------------------------------------------------------------------------
# collect_and_store
# ---------------------------------------------------------------------------

class TestCollectAndStore:
    def test_stores_to_db_and_publishes(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with (
            patch("requests.get", return_value=_mock_response(_av_response())),
            patch("collectors.alpha_vantage.SessionLocal", return_value=mock_session),
            patch("collectors.alpha_vantage.publish") as mock_publish,
        ):
            from collectors.alpha_vantage import collect_and_store

            collect_and_store(interval="5min")

        assert mock_session.add.call_count == 2
        mock_session.commit.assert_called_once()

        mock_publish.assert_called_once()
        stream_arg, data_arg = mock_publish.call_args.args
        assert stream_arg == "prices.brent"
        assert data_arg["source"] == "alpha_vantage"
        assert data_arg["timeframe"] == "5min"

    def test_no_publish_when_empty(self):
        empty_resp = {"Meta Data": {}, "Time Series (5min)": {}}
        with (
            patch("requests.get", return_value=_mock_response(empty_resp)),
            patch("collectors.alpha_vantage.publish") as mock_publish,
        ):
            from collectors.alpha_vantage import collect_and_store

            collect_and_store(interval="5min")

        mock_publish.assert_not_called()
