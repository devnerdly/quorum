"""Tests for the OPEC MOMR collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Sample HTML
# ---------------------------------------------------------------------------

_SAMPLE_HTML = """
<html>
<head><title>OPEC Monthly Oil Market Report - April 2024</title></head>
<body>
<h1>Monthly Oil Market Report</h1>
<p>This report covers the month of <strong>April 2024</strong>.</p>
<p>OPEC crude oil production averaged 26.80 mb/d in March 2024,
   according to secondary sources.</p>
<p>World oil demand is forecast to grow by 2.2 mb/d in 2024 to average
   102.87 mb/d, supported by robust growth in non-OECD countries.</p>
<p>Non-OPEC liquids supply is projected to grow by 1.4 mb/d in 2024 to
   average 53.15 mb/d.</p>
</body>
</html>
"""

_SAMPLE_HTML_NO_NUMBERS = """
<html>
<body>
<p>Welcome to the OPEC website. March 2025 report.</p>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# fetch_opec_momr
# ---------------------------------------------------------------------------

class TestFetchOpecMomr:
    def test_calls_correct_url(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp) as mock_get:
            from collectors.opec import fetch_opec_momr, _OPEC_URL

            result = fetch_opec_momr()

        call_args = mock_get.call_args
        assert call_args.args[0] == _OPEC_URL

    def test_extracts_production(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp):
            from collectors.opec import fetch_opec_momr

            result = fetch_opec_momr()

        assert result["total_production"] == pytest.approx(26.80)

    def test_extracts_demand_forecast(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp):
            from collectors.opec import fetch_opec_momr

            result = fetch_opec_momr()

        assert result["demand_forecast"] == pytest.approx(102.87)

    def test_extracts_supply_forecast(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp):
            from collectors.opec import fetch_opec_momr

            result = fetch_opec_momr()

        assert result["supply_forecast"] == pytest.approx(53.15)

    def test_extracts_report_date(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp):
            from collectors.opec import fetch_opec_momr

            result = fetch_opec_momr()

        assert isinstance(result["report_date"], datetime)
        assert result["report_date"].year == 2024
        assert result["report_date"].month == 4

    def test_raw_text_is_stripped_html(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp):
            from collectors.opec import fetch_opec_momr

            result = fetch_opec_momr()

        assert "<html>" not in result["raw_text"]
        assert "<p>" not in result["raw_text"]
        assert "OPEC" in result["raw_text"]

    def test_no_numbers_returns_none_fields(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML_NO_NUMBERS
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp):
            from collectors.opec import fetch_opec_momr

            result = fetch_opec_momr()

        assert result["total_production"] is None
        assert result["demand_forecast"] is None
        assert result["supply_forecast"] is None

    def test_raw_text_capped_at_50k_chars(self):
        large_html = "<p>" + "x" * 100_000 + "</p>"
        mock_resp = MagicMock()
        mock_resp.text = large_html
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.opec.requests.get", return_value=mock_resp):
            from collectors.opec import fetch_opec_momr

            result = fetch_opec_momr()

        assert len(result["raw_text"]) <= 50_000


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
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.opec.requests.get", return_value=mock_resp),
            patch("collectors.opec.SessionLocal", return_value=mock_session),
            patch("collectors.opec.publish") as mock_publish,
        ):
            from collectors.opec import collect_and_store

            collect_and_store()

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

        mock_publish.assert_called_once()
        stream_arg, data_arg = mock_publish.call_args.args
        assert stream_arg == "macro.opec"
        assert data_arg["dataset"] == "opec"

    def test_stored_model_fields(self):
        """Verify MacroOPEC model is constructed with correct field values."""
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        from collectors.opec import collect_and_store
        from shared.models.macro import MacroOPEC

        stored_rows: list = []

        def capture_add(row):
            stored_rows.append(row)

        mock_session.add.side_effect = capture_add

        with (
            patch("collectors.opec.requests.get", return_value=mock_resp),
            patch("collectors.opec.SessionLocal", return_value=mock_session),
            patch("collectors.opec.publish"),
        ):
            collect_and_store()

        assert len(stored_rows) == 1
        row = stored_rows[0]
        assert isinstance(row, MacroOPEC)
        assert row.total_production == pytest.approx(26.80)
        assert row.demand_forecast == pytest.approx(102.87)
        assert row.supply_forecast == pytest.approx(53.15)
        assert row.raw_text is not None
        assert "<html>" not in row.raw_text

    def test_published_event_data(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.opec.requests.get", return_value=mock_resp),
            patch("collectors.opec.SessionLocal", return_value=mock_session),
            patch("collectors.opec.publish") as mock_publish,
        ):
            from collectors.opec import collect_and_store

            collect_and_store()

        _, data_arg = mock_publish.call_args.args
        event_data = data_arg["data"]
        assert event_data["total_production"] == pytest.approx(26.80)
        assert event_data["demand_forecast"] == pytest.approx(102.87)
        assert event_data["supply_forecast"] == pytest.approx(53.15)
        # raw_text should NOT be in the Redis event (too large)
        assert "raw_text" not in event_data
