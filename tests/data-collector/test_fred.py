"""Tests for the FRED macro series collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

def _fred_response(value: str = "5.33", date: str = "2024-01-01") -> dict:
    return {
        "realtime_start": "2024-01-10",
        "realtime_end": "2024-01-10",
        "observation_start": "2000-01-01",
        "observation_end": "9999-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "desc",
        "count": 1,
        "offset": 0,
        "limit": 1,
        "observations": [
            {
                "realtime_start": "2024-01-10",
                "realtime_end": "2024-01-10",
                "date": date,
                "value": value,
            }
        ],
    }


# ---------------------------------------------------------------------------
# parse_fred_series
# ---------------------------------------------------------------------------

class TestParseFredSeries:
    def test_extracts_latest_observation(self):
        from collectors.fred import parse_fred_series

        result = parse_fred_series(_fred_response("5.33", "2024-01-01"))
        assert result is not None
        assert result["value"] == pytest.approx(5.33)

    def test_date_is_utc_datetime(self):
        from collectors.fred import parse_fred_series

        result = parse_fred_series(_fred_response("5.33", "2024-03-15"))
        assert isinstance(result["date"], datetime)
        assert result["date"].tzinfo == timezone.utc
        assert result["date"].year == 2024
        assert result["date"].month == 3
        assert result["date"].day == 15

    def test_missing_value_dot_returns_none(self):
        """FRED uses '.' as a placeholder for missing values."""
        from collectors.fred import parse_fred_series

        result = parse_fred_series(_fred_response(value=".", date="2024-01-01"))
        assert result is not None
        assert result["value"] is None

    def test_empty_observations_returns_none(self):
        from collectors.fred import parse_fred_series

        result = parse_fred_series({"observations": []})
        assert result is None

    def test_multiple_observations_returns_last(self):
        """The latest observation is the last item in the list."""
        from collectors.fred import parse_fred_series

        data = {
            "observations": [
                {"date": "2023-12-01", "value": "4.0"},
                {"date": "2024-01-01", "value": "5.33"},
            ]
        }
        result = parse_fred_series(data)
        assert result["value"] == pytest.approx(5.33)

    def test_negative_value(self):
        from collectors.fred import parse_fred_series

        result = parse_fred_series(_fred_response("-0.25", "2020-03-01"))
        assert result["value"] == pytest.approx(-0.25)


# ---------------------------------------------------------------------------
# fetch_fred_series
# ---------------------------------------------------------------------------

class TestFetchFredSeries:
    def test_calls_correct_url(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _fred_response("5.33")
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.fred.requests.get", return_value=mock_resp) as mock_get:
            from collectors.fred import fetch_fred_series, _FRED_URL

            result = fetch_fred_series("FEDFUNDS")

        call_args = mock_get.call_args
        assert call_args.args[0] == _FRED_URL
        assert call_args.kwargs["params"]["series_id"] == "FEDFUNDS"

    def test_returns_parsed_observation(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _fred_response("5.33", "2024-01-01")
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.fred.requests.get", return_value=mock_resp):
            from collectors.fred import fetch_fred_series

            result = fetch_fred_series("FEDFUNDS")

        assert result is not None
        assert result["value"] == pytest.approx(5.33)


# ---------------------------------------------------------------------------
# collect_and_store
# ---------------------------------------------------------------------------

class TestCollectAndStore:
    def _make_mock_session(self) -> MagicMock:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        return mock_session

    def test_stores_all_series_and_publishes(self):
        from collectors.fred import FRED_SERIES

        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.json.return_value = _fred_response("5.33")
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.fred.requests.get", return_value=mock_resp),
            patch("collectors.fred.SessionLocal", return_value=mock_session),
            patch("collectors.fred.publish") as mock_publish,
        ):
            from collectors.fred import collect_and_store

            collect_and_store()

        # One DB row added per series
        assert mock_session.add.call_count == len(FRED_SERIES)
        mock_session.commit.assert_called_once()

        # One publish call for the batch
        mock_publish.assert_called_once()
        stream_arg, data_arg = mock_publish.call_args.args
        assert stream_arg == "macro.fred"
        assert data_arg["dataset"] == "fred"

    def test_skips_failed_series(self):
        """A single failing series should not abort the whole collection."""
        mock_session = self._make_mock_session()

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("network error")
            mock_resp = MagicMock()
            mock_resp.json.return_value = _fred_response("5.33")
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        with (
            patch("collectors.fred.requests.get", side_effect=side_effect),
            patch("collectors.fred.SessionLocal", return_value=mock_session),
            patch("collectors.fred.publish"),
        ):
            from collectors.fred import collect_and_store, FRED_SERIES

            collect_and_store()

        # Should store (FRED_SERIES count - 1) rows since first call raises
        assert mock_session.add.call_count == len(FRED_SERIES) - 1

    def test_published_data_contains_series_values(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.json.return_value = _fred_response("5.33")
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.fred.requests.get", return_value=mock_resp),
            patch("collectors.fred.SessionLocal", return_value=mock_session),
            patch("collectors.fred.publish") as mock_publish,
        ):
            from collectors.fred import collect_and_store, FRED_SERIES

            collect_and_store()

        _, data_arg = mock_publish.call_args.args
        for series_id in FRED_SERIES:
            assert series_id in data_arg["data"]
