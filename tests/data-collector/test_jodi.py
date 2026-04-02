"""Tests for the JODI oil statistics collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Sample CSV data
# ---------------------------------------------------------------------------

_SAMPLE_CSV = """\
COUNTRY,PRODUCT,FLOW,UNIT,DATE,VALUE
USA,CRUDEOIL,PRODUCTION,KB/D,2024-01,12500.5
USA,CRUDEOIL,IMPORTS,KB/D,2024-01,6000.0
SAU,CRUDEOIL,PRODUCTION,KB/D,2024-01,9800.0
RUS,CRUDEOIL,PRODUCTION,KB/D,2024-01,10200.0
USA,CRUDEOIL,PRODUCTION,KB/D,2023-12,12300.0
"""

_SAMPLE_CSV_MISSING_VALUES = """\
COUNTRY,PRODUCT,FLOW,UNIT,DATE,VALUE
USA,CRUDEOIL,PRODUCTION,KB/D,2024-01,x
USA,CRUDEOIL,IMPORTS,KB/D,2024-01,..
USA,CRUDEOIL,EXPORTS,KB/D,2024-01,
"""

_SAMPLE_CSV_YYYYMM_FORMAT = """\
COUNTRY,PRODUCT,FLOW,UNIT,DATE,VALUE
USA,CRUDEOIL,PRODUCTION,KB/D,202401,12500.5
"""


# ---------------------------------------------------------------------------
# parse_jodi_csv
# ---------------------------------------------------------------------------

class TestParseJodiCsv:
    def test_returns_correct_count(self):
        from collectors.jodi import parse_jodi_csv

        result = parse_jodi_csv(_SAMPLE_CSV)
        assert len(result) == 5

    def test_record_fields(self):
        from collectors.jodi import parse_jodi_csv

        result = parse_jodi_csv(_SAMPLE_CSV)
        rec = result[0]
        assert rec["country"] == "USA"
        assert rec["product"] == "CRUDEOIL"
        assert rec["flow"] == "PRODUCTION"
        assert rec["value"] == pytest.approx(12500.5)
        assert isinstance(rec["timestamp"], datetime)
        assert rec["timestamp"].tzinfo == timezone.utc

    def test_date_parsed_yyyy_mm(self):
        from collectors.jodi import parse_jodi_csv

        result = parse_jodi_csv(_SAMPLE_CSV)
        assert result[0]["timestamp"].year == 2024
        assert result[0]["timestamp"].month == 1

    def test_date_parsed_yyyymm(self):
        from collectors.jodi import parse_jodi_csv

        result = parse_jodi_csv(_SAMPLE_CSV_YYYYMM_FORMAT)
        assert len(result) == 1
        assert result[0]["timestamp"].year == 2024
        assert result[0]["timestamp"].month == 1

    def test_missing_values_become_none(self):
        from collectors.jodi import parse_jodi_csv

        result = parse_jodi_csv(_SAMPLE_CSV_MISSING_VALUES)
        # All rows have missing values (x, .., empty)
        for rec in result:
            assert rec["value"] is None

    def test_empty_csv_returns_empty_list(self):
        from collectors.jodi import parse_jodi_csv

        result = parse_jodi_csv("COUNTRY,PRODUCT,FLOW,UNIT,DATE,VALUE\n")
        assert result == []

    def test_whitespace_headers_tolerated(self):
        """Headers with leading/trailing spaces should still be parsed."""
        from collectors.jodi import parse_jodi_csv

        csv_with_spaces = " COUNTRY , PRODUCT , FLOW , UNIT , DATE , VALUE \nUSA,CRUDEOIL,PRODUCTION,KB/D,2024-01,100.0\n"
        result = parse_jodi_csv(csv_with_spaces)
        assert len(result) == 1
        assert result[0]["country"] == "USA"


# ---------------------------------------------------------------------------
# fetch_jodi
# ---------------------------------------------------------------------------

class TestFetchJodi:
    def test_calls_correct_url(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_CSV
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.jodi.requests.get", return_value=mock_resp) as mock_get:
            from collectors.jodi import fetch_jodi, _JODI_CSV_URL

            result = fetch_jodi()

        call_args = mock_get.call_args
        assert call_args.args[0] == _JODI_CSV_URL

    def test_returns_parsed_records(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_CSV
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.jodi.requests.get", return_value=mock_resp):
            from collectors.jodi import fetch_jodi

            result = fetch_jodi()

        assert len(result) == 5
        assert result[0]["country"] == "USA"


# ---------------------------------------------------------------------------
# collect_and_store
# ---------------------------------------------------------------------------

class TestCollectAndStore:
    def _make_mock_session(self) -> MagicMock:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        return mock_session

    def test_stores_all_records_and_publishes(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_CSV
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.jodi.requests.get", return_value=mock_resp),
            patch("collectors.jodi.SessionLocal", return_value=mock_session),
            patch("collectors.jodi.publish") as mock_publish,
        ):
            from collectors.jodi import collect_and_store

            collect_and_store()

        assert mock_session.add.call_count == 5
        mock_session.commit.assert_called_once()

        mock_publish.assert_called_once()
        stream_arg, data_arg = mock_publish.call_args.args
        assert stream_arg == "macro.jodi"
        assert data_arg["dataset"] == "jodi"
        assert data_arg["data"]["records_stored"] == 5

    def test_no_store_when_empty(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.text = "COUNTRY,PRODUCT,FLOW,UNIT,DATE,VALUE\n"
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.jodi.requests.get", return_value=mock_resp),
            patch("collectors.jodi.SessionLocal", return_value=mock_session),
            patch("collectors.jodi.publish") as mock_publish,
        ):
            from collectors.jodi import collect_and_store

            collect_and_store()

        mock_session.add.assert_not_called()
        mock_publish.assert_not_called()
