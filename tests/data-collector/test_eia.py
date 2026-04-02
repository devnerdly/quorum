"""Tests for the EIA inventory collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures / sample data
# ---------------------------------------------------------------------------

_SAMPLE_EIA_RESPONSE = {
    "response": {
        "data": [
            # Latest week — crude total (WCRSTUS1)
            {
                "period": "2024-01-05",
                "series": "WCRSTUS1",
                "series-description": "U.S. Ending Stocks of Crude Oil",
                "duoarea": "NUS",
                "value": 432_000,
                "units": "MBBL",
            },
            # Previous week — crude total
            {
                "period": "2023-12-29",
                "series": "WCRSTUS1",
                "series-description": "U.S. Ending Stocks of Crude Oil",
                "duoarea": "NUS",
                "value": 430_000,
                "units": "MBBL",
            },
            # Latest week — SPR (WCSSTUS1)
            {
                "period": "2024-01-05",
                "series": "WCSSTUS1",
                "series-description": "U.S. Ending Stocks of Crude Oil in SPR",
                "duoarea": "NUS",
                "value": 350_000,
                "units": "MBBL",
            },
            # Latest week — Cushing (WCUOK1)
            {
                "period": "2024-01-05",
                "series": "WCUOK1",
                "series-description": "Cushing, OK Ending Stocks of Crude Oil",
                "duoarea": "Y35NY",
                "value": 22_000,
                "units": "MBBL",
            },
        ]
    }
}


# ---------------------------------------------------------------------------
# parse_eia_response
# ---------------------------------------------------------------------------

class TestParseEiaResponse:
    def test_returns_crude_inventory_total(self):
        from collectors.eia import parse_eia_response

        result = parse_eia_response(_SAMPLE_EIA_RESPONSE)
        assert result["crude_inventory_total"] == pytest.approx(432_000)

    def test_computes_change(self):
        from collectors.eia import parse_eia_response

        result = parse_eia_response(_SAMPLE_EIA_RESPONSE)
        # 432_000 - 430_000 = 2_000
        assert result["crude_inventory_change"] == pytest.approx(2_000)

    def test_returns_spr_inventory(self):
        from collectors.eia import parse_eia_response

        result = parse_eia_response(_SAMPLE_EIA_RESPONSE)
        assert result["spr_inventory"] == pytest.approx(350_000)

    def test_returns_cushing_inventory(self):
        from collectors.eia import parse_eia_response

        result = parse_eia_response(_SAMPLE_EIA_RESPONSE)
        assert result["cushing_inventory"] == pytest.approx(22_000)

    def test_report_date_parsed(self):
        from collectors.eia import parse_eia_response

        result = parse_eia_response(_SAMPLE_EIA_RESPONSE)
        assert isinstance(result.get("report_date"), datetime)
        assert result["report_date"].year == 2024
        assert result["report_date"].month == 1
        assert result["report_date"].day == 5

    def test_empty_response_returns_empty_dict(self):
        from collectors.eia import parse_eia_response

        result = parse_eia_response({"response": {"data": []}})
        assert result == {}

    def test_missing_response_key(self):
        from collectors.eia import parse_eia_response

        result = parse_eia_response({})
        assert result == {}

    def test_null_value_handled(self):
        from collectors.eia import parse_eia_response

        data = {
            "response": {
                "data": [
                    {
                        "period": "2024-01-05",
                        "series": "WCRSTUS1",
                        "value": None,
                    }
                ]
            }
        }
        result = parse_eia_response(data)
        assert result.get("crude_inventory_total") is None


# ---------------------------------------------------------------------------
# fetch_eia_inventories
# ---------------------------------------------------------------------------

class TestFetchEiaInventories:
    def test_calls_correct_url(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_EIA_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.eia.requests.get", return_value=mock_resp) as mock_get:
            from collectors.eia import fetch_eia_inventories, _EIA_URL

            result = fetch_eia_inventories()

        call_args = mock_get.call_args
        assert call_args.args[0] == _EIA_URL
        assert "crude_inventory_total" in result

    def test_returns_parsed_data(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _SAMPLE_EIA_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("collectors.eia.requests.get", return_value=mock_resp):
            from collectors.eia import fetch_eia_inventories

            result = fetch_eia_inventories()

        assert result["crude_inventory_total"] == pytest.approx(432_000)
        assert result["crude_inventory_change"] == pytest.approx(2_000)


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
        mock_resp.json.return_value = _SAMPLE_EIA_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.eia.requests.get", return_value=mock_resp),
            patch("collectors.eia.SessionLocal", return_value=mock_session),
            patch("collectors.eia.publish") as mock_publish,
        ):
            from collectors.eia import collect_and_store

            collect_and_store()

        # DB row added and committed
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()

        # Event published to correct stream
        mock_publish.assert_called_once()
        stream_arg, data_arg = mock_publish.call_args.args
        assert stream_arg == "macro.eia"
        assert data_arg["dataset"] == "eia"

    def test_no_store_when_no_data(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"response": {"data": []}}
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.eia.requests.get", return_value=mock_resp),
            patch("collectors.eia.SessionLocal", return_value=mock_session),
            patch("collectors.eia.publish") as mock_publish,
        ):
            from collectors.eia import collect_and_store

            collect_and_store()

        mock_session.add.assert_not_called()
        mock_publish.assert_not_called()
