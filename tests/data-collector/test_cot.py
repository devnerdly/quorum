"""Tests for the CFTC Commitment of Traders (COT) collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

# Row order per Nasdaq CFTC_F_ALL layout:
# [Date, OpenInterest, NC_Long, NC_Short, NC_Spreading, C_Long, C_Short,
#  Total_Long, Total_Short, NR_Long, NR_Short]
_SAMPLE_ROW = [
    "2024-01-02",  # 0: Date
    1_500_000,     # 1: Open Interest
    350_000,       # 2: Non-commercial Long
    180_000,       # 3: Non-commercial Short
    40_000,        # 4: Non-commercial Spreading
    700_000,       # 5: Commercial Long
    820_000,       # 6: Commercial Short
    1_090_000,     # 7: Total Long
    1_040_000,     # 8: Total Short
    50_000,        # 9: Non-reportable Long
    60_000,        # 10: Non-reportable Short
]

_SAMPLE_COT_RESPONSE = {
    "dataset": {
        "id": 13_500_370,
        "dataset_code": "067651_F_ALL",
        "database_code": "CFTC",
        "name": "Crude Oil, Light Sweet - Chicago Mercantile Exchange",
        "description": "...",
        "refreshed_at": "2024-01-06T12:00:00.000Z",
        "newest_available_date": "2024-01-02",
        "oldest_available_date": "1986-01-07",
        "column_names": [
            "Date", "Open Interest",
            "Noncommercial Long", "Noncommercial Short", "Noncommercial Spreading",
            "Commercial Long", "Commercial Short",
            "Total Long", "Total Short",
            "Nonreportable Positions Long", "Nonreportable Positions Short",
        ],
        "frequency": "weekly",
        "type": "Time Series",
        "premium": False,
        "data": [_SAMPLE_ROW],
    }
}


# ---------------------------------------------------------------------------
# parse_cot_row
# ---------------------------------------------------------------------------

class TestParseCotRow:
    def test_commercial_long_short(self):
        from collectors.cot import parse_cot_row

        result = parse_cot_row(_SAMPLE_ROW)
        assert result["commercial_long"] == pytest.approx(700_000)
        assert result["commercial_short"] == pytest.approx(820_000)

    def test_non_commercial_long_short(self):
        from collectors.cot import parse_cot_row

        result = parse_cot_row(_SAMPLE_ROW)
        assert result["non_commercial_long"] == pytest.approx(350_000)
        assert result["non_commercial_short"] == pytest.approx(180_000)

    def test_open_interest(self):
        from collectors.cot import parse_cot_row

        result = parse_cot_row(_SAMPLE_ROW)
        assert result["open_interest"] == pytest.approx(1_500_000)

    def test_commercial_net_computed(self):
        from collectors.cot import parse_cot_row

        result = parse_cot_row(_SAMPLE_ROW)
        # 700_000 - 820_000 = -120_000
        assert result["commercial_net"] == pytest.approx(-120_000)

    def test_non_commercial_net_computed(self):
        from collectors.cot import parse_cot_row

        result = parse_cot_row(_SAMPLE_ROW)
        # 350_000 - 180_000 = 170_000
        assert result["non_commercial_net"] == pytest.approx(170_000)

    def test_report_date_parsed(self):
        from collectors.cot import parse_cot_row

        result = parse_cot_row(_SAMPLE_ROW)
        assert isinstance(result["report_date"], datetime)
        assert result["report_date"].year == 2024
        assert result["report_date"].month == 1
        assert result["report_date"].day == 2
        assert result["report_date"].tzinfo == timezone.utc

    def test_none_values_handled(self):
        from collectors.cot import parse_cot_row

        row_with_none = ["2024-01-02", None, None, None, None, None, None, None, None, None, None]
        result = parse_cot_row(row_with_none)
        assert result["commercial_long"] is None
        assert result["commercial_short"] is None
        assert result["commercial_net"] is None

    def test_short_row_handled(self):
        from collectors.cot import parse_cot_row

        # Partial row should not raise
        result = parse_cot_row(["2024-01-02"])
        assert result["report_date"] is not None
        assert result["commercial_long"] is None


# ---------------------------------------------------------------------------
# fetch_cot
# ---------------------------------------------------------------------------

class TestFetchCot:
    def test_calls_correct_url(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_COT_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.cot.requests.get", return_value=mock_resp) as mock_get:
            from collectors.cot import fetch_cot, _COT_URL

            result = fetch_cot()

        call_args = mock_get.call_args
        assert call_args.args[0] == _COT_URL

    def test_returns_parsed_row(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_COT_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.cot.requests.get", return_value=mock_resp):
            from collectors.cot import fetch_cot

            result = fetch_cot()

        assert result["commercial_long"] == pytest.approx(700_000)
        assert result["non_commercial_net"] == pytest.approx(170_000)

    def test_empty_data_returns_empty_dict(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"dataset": {"data": []}}
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.cot.requests.get", return_value=mock_resp):
            from collectors.cot import fetch_cot

            result = fetch_cot()

        assert result == {}


# ---------------------------------------------------------------------------
# collect_and_store
# ---------------------------------------------------------------------------

class TestCollectAndStore:
    def _make_mock_session(self) -> MagicMock:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        return mock_session

    def test_stores_to_db_and_publishes(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_COT_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.cot.requests.get", return_value=mock_resp),
            patch("collectors.cot.SessionLocal", return_value=mock_session),
            patch("collectors.cot.publish") as mock_publish,
        ):
            from collectors.cot import collect_and_store

            collect_and_store()

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

        mock_publish.assert_called_once()
        stream_arg, data_arg = mock_publish.call_args.args
        assert stream_arg == "macro.cot"
        assert data_arg["dataset"] == "cot"

    def test_no_store_when_empty(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"dataset": {"data": []}}
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.cot.requests.get", return_value=mock_resp),
            patch("collectors.cot.SessionLocal", return_value=mock_session),
            patch("collectors.cot.publish") as mock_publish,
        ):
            from collectors.cot import collect_and_store

            collect_and_store()

        mock_session.add.assert_not_called()
        mock_publish.assert_not_called()

    def test_stored_model_fields(self):
        """Verify the MacroCOT model is constructed with the correct fields."""
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_COT_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        from collectors.cot import collect_and_store
        from shared.models.macro import MacroCOT

        stored_rows: list = []

        def capture_add(row):
            stored_rows.append(row)

        mock_session.add.side_effect = capture_add

        with (
            patch("collectors.cot.requests.get", return_value=mock_resp),
            patch("collectors.cot.SessionLocal", return_value=mock_session),
            patch("collectors.cot.publish"),
        ):
            collect_and_store()

        assert len(stored_rows) == 1
        row = stored_rows[0]
        assert isinstance(row, MacroCOT)
        assert row.commercial_long == pytest.approx(700_000)
        assert row.non_commercial_long == pytest.approx(350_000)
        assert row.open_interest == pytest.approx(1_500_000)
