"""Tests for the Datalastic AIS shipping collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# classify_tanker
# ---------------------------------------------------------------------------

class TestClassifyTanker:
    def test_vlcc_by_dwt(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker(None, 250_000) == "VLCC"

    def test_vlcc_exact_boundary(self):
        from collectors.shipping import classify_tanker

        # Exactly 200K is NOT > 200K → falls into Suezmax range
        assert classify_tanker(None, 200_000) == "Suezmax"

    def test_vlcc_above_boundary(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker(None, 200_001) == "VLCC"

    def test_suezmax_by_dwt(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker(None, 150_000) == "Suezmax"

    def test_suezmax_boundary(self):
        from collectors.shipping import classify_tanker

        # Exactly 120K is NOT > 120K → Aframax
        assert classify_tanker(None, 120_000) == "Aframax"

    def test_aframax_by_dwt(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker(None, 100_000) == "Aframax"

    def test_aframax_boundary(self):
        from collectors.shipping import classify_tanker

        # Exactly 80K is NOT > 80K → None (below all thresholds)
        assert classify_tanker(None, 80_000) is None

    def test_below_aframax_returns_none(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker(None, 50_000) is None

    def test_vlcc_by_type_name(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker("VLCC Tanker", None) == "VLCC"

    def test_suezmax_by_type_name(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker("Suezmax", None) == "Suezmax"

    def test_aframax_by_type_name(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker("Aframax crude carrier", None) == "Aframax"

    def test_generic_tanker_by_type_name(self):
        from collectors.shipping import classify_tanker

        result = classify_tanker("Crude Oil Tanker", None)
        # Should return a non-None value for a generic crude tanker
        assert result is not None

    def test_dwt_takes_priority_over_type_name(self):
        from collectors.shipping import classify_tanker

        # DWT says Suezmax, type_name says VLCC — DWT wins
        assert classify_tanker("VLCC", 150_000) == "Suezmax"

    def test_none_type_and_none_dwt(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker(None, None) is None

    def test_type_name_case_insensitive(self):
        from collectors.shipping import classify_tanker

        assert classify_tanker("vlcc", None) == "VLCC"
        assert classify_tanker("SUEZMAX", None) == "Suezmax"


# ---------------------------------------------------------------------------
# infer_status
# ---------------------------------------------------------------------------

class TestInferStatus:
    def test_anchored_below_threshold(self):
        from collectors.shipping import infer_status

        assert infer_status(0.3, None) == "anchored"

    def test_anchored_exactly_zero(self):
        from collectors.shipping import infer_status

        assert infer_status(0.0, None) == "anchored"

    def test_anchored_at_threshold(self):
        from collectors.shipping import infer_status

        # 0.5 is NOT < 0.5 → laden
        assert infer_status(0.5, None) == "laden"

    def test_laden_slow_speed(self):
        from collectors.shipping import infer_status

        assert infer_status(8.0, None) == "laden"

    def test_laden_at_upper_boundary(self):
        from collectors.shipping import infer_status

        assert infer_status(10.0, None) == "laden"

    def test_ballast_above_threshold(self):
        from collectors.shipping import infer_status

        assert infer_status(10.1, None) == "ballast"

    def test_ballast_fast_speed(self):
        from collectors.shipping import infer_status

        assert infer_status(15.0, None) == "ballast"

    def test_nav_status_anchor_overrides_speed(self):
        from collectors.shipping import infer_status

        # High speed but nav_status says anchored — nav_status wins
        assert infer_status(12.0, "at anchor") == "anchored"

    def test_nav_status_moored(self):
        from collectors.shipping import infer_status

        assert infer_status(0.0, "moored") == "anchored"

    def test_none_speed_returns_unknown(self):
        from collectors.shipping import infer_status

        assert infer_status(None, None) == "unknown"

    def test_nav_status_anchor_with_none_speed(self):
        from collectors.shipping import infer_status

        assert infer_status(None, "at anchor") == "anchored"


# ---------------------------------------------------------------------------
# parse_vessel_data
# ---------------------------------------------------------------------------

_SAMPLE_VESSEL = {
    "name": "CRUDE TITAN",
    "imo": "9876543",
    "vessel_type": "Crude Oil Tanker",
    "lat": 26.5,
    "lon": 56.8,
    "speed": 4.5,
    "nav_status": "under way using engine",
    "dwt": 300_000,
    "destination": "ROTTERDAM",
    "eta": "2026-04-15T08:00:00",
}


class TestParseVesselData:
    def test_vessel_name_parsed(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        assert result["vessel_name"] == "CRUDE TITAN"

    def test_imo_parsed(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        assert result["imo"] == "9876543"

    def test_coordinates_parsed(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        assert result["latitude"] == pytest.approx(26.5)
        assert result["longitude"] == pytest.approx(56.8)

    def test_speed_parsed(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        assert result["speed"] == pytest.approx(4.5)

    def test_tanker_class_derived(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        # DWT 300K → VLCC
        assert result["tanker_class"] == "VLCC"

    def test_status_derived(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        # speed 4.5 → laden
        assert result["status"] == "laden"

    def test_chokepoint_detected(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        # lat=26.5, lon=56.8 is inside hormuz bounding box
        assert result["chokepoint"] == "hormuz"

    def test_destination_parsed(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        assert result["destination"] == "ROTTERDAM"

    def test_eta_parsed_as_datetime(self):
        from collectors.shipping import parse_vessel_data

        result = parse_vessel_data(_SAMPLE_VESSEL)
        assert isinstance(result["eta"], datetime)
        assert result["eta"].year == 2026
        assert result["eta"].month == 4
        assert result["eta"].day == 15

    def test_missing_imo_returns_none(self):
        from collectors.shipping import parse_vessel_data

        raw = {**_SAMPLE_VESSEL}
        raw.pop("imo")
        result = parse_vessel_data(raw)
        assert result["imo"] is None

    def test_no_chokepoint_returns_none(self):
        from collectors.shipping import parse_vessel_data

        raw = {**_SAMPLE_VESSEL, "lat": 0.0, "lon": 0.0}
        result = parse_vessel_data(raw)
        assert result["chokepoint"] is None

    def test_anchored_vessel_status(self):
        from collectors.shipping import parse_vessel_data

        raw = {**_SAMPLE_VESSEL, "speed": 0.2}
        result = parse_vessel_data(raw)
        assert result["status"] == "anchored"

    def test_ballast_vessel_status(self):
        from collectors.shipping import parse_vessel_data

        raw = {**_SAMPLE_VESSEL, "speed": 13.0}
        result = parse_vessel_data(raw)
        assert result["status"] == "ballast"

    def test_alternative_field_names(self):
        """parse_vessel_data should handle alternative AIS field naming."""
        from collectors.shipping import parse_vessel_data

        raw = {
            "vessel_name": "ALT NAME",
            "latitude": 30.0,
            "longitude": 32.5,
            "sog": 6.0,
            "type_name": "Crude Oil Tanker",
        }
        result = parse_vessel_data(raw)
        assert result["vessel_name"] == "ALT NAME"
        assert result["latitude"] == pytest.approx(30.0)
        assert result["speed"] == pytest.approx(6.0)


# ---------------------------------------------------------------------------
# compute_shipping_metrics
# ---------------------------------------------------------------------------

_POSITIONS = [
    # VLCC anchored in Hormuz → floating_storage, hormuz_traffic, anchored_count, total_vlcc_count
    {
        "tanker_class": "VLCC",
        "status": "anchored",
        "chokepoint": "hormuz",
    },
    # VLCC laden in Suez → suez_traffic, total_vlcc_count
    {
        "tanker_class": "VLCC",
        "status": "laden",
        "chokepoint": "suez",
    },
    # Suezmax anchored (no chokepoint) → floating_storage, anchored_count
    {
        "tanker_class": "Suezmax",
        "status": "anchored",
        "chokepoint": None,
    },
    # Aframax ballast in Malacca → malacca_traffic
    {
        "tanker_class": "Aframax",
        "status": "ballast",
        "chokepoint": "malacca",
    },
    # Generic tanker, no chokepoint
    {
        "tanker_class": "Tanker",
        "status": "laden",
        "chokepoint": None,
    },
]


class TestComputeShippingMetrics:
    def test_total_vlcc_count(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics(_POSITIONS)
        # Two VLCCs in the list
        assert metrics["total_vlcc_count"] == pytest.approx(2.0)

    def test_anchored_count(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics(_POSITIONS)
        # VLCC anchored + Suezmax anchored = 2
        assert metrics["anchored_count"] == pytest.approx(2.0)

    def test_floating_storage(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics(_POSITIONS)
        # VLCC anchored + Suezmax anchored = 2
        assert metrics["floating_storage"] == pytest.approx(2.0)

    def test_hormuz_traffic(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics(_POSITIONS)
        assert metrics["hormuz_traffic"] == pytest.approx(1.0)

    def test_suez_traffic(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics(_POSITIONS)
        assert metrics["suez_traffic"] == pytest.approx(1.0)

    def test_malacca_traffic(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics(_POSITIONS)
        assert metrics["malacca_traffic"] == pytest.approx(1.0)

    def test_empty_positions(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics([])
        assert metrics["total_vlcc_count"] == pytest.approx(0.0)
        assert metrics["floating_storage"] == pytest.approx(0.0)
        assert metrics["anchored_count"] == pytest.approx(0.0)

    def test_aframax_not_counted_as_floating_storage(self):
        """Only VLCC and Suezmax anchored vessels count as floating storage."""
        from collectors.shipping import compute_shipping_metrics

        positions = [
            {"tanker_class": "Aframax", "status": "anchored", "chokepoint": None},
        ]
        metrics = compute_shipping_metrics(positions)
        assert metrics["floating_storage"] == pytest.approx(0.0)
        assert metrics["anchored_count"] == pytest.approx(1.0)

    def test_all_metrics_present(self):
        from collectors.shipping import compute_shipping_metrics

        metrics = compute_shipping_metrics(_POSITIONS)
        expected_keys = {
            "floating_storage",
            "hormuz_traffic",
            "suez_traffic",
            "malacca_traffic",
            "total_vlcc_count",
            "anchored_count",
        }
        assert expected_keys == set(metrics.keys())


# ---------------------------------------------------------------------------
# collect_and_store (integration-style, fully mocked)
# ---------------------------------------------------------------------------

class TestCollectAndStore:
    def _make_mock_session(self) -> MagicMock:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        return mock_session

    def _sample_api_response(self) -> dict:
        return {
            "data": [
                {
                    "name": "TEST VLCC",
                    "imo": "1234567",
                    "vessel_type": "Crude Oil Tanker",
                    "lat": 26.5,
                    "lon": 56.8,
                    "speed": 0.2,
                    "nav_status": "at anchor",
                    "dwt": 280_000,
                    "destination": "FUJAIRAH",
                    "eta": None,
                }
            ]
        }

    def test_stores_position_and_metrics(self):
        mock_session = self._make_mock_session()
        mock_resp = MagicMock()
        mock_resp.json.return_value = self._sample_api_response()
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("collectors.shipping.settings") as mock_settings,
            patch("collectors.shipping.requests.get", return_value=mock_resp),
            patch("collectors.shipping.SessionLocal", return_value=mock_session),
        ):
            mock_settings.datalastic_api_key = "test-key"
            from collectors.shipping import collect_and_store

            collect_and_store()

        # Should have added: 1 ShippingPosition + 6 ShippingMetric rows
        assert mock_session.add.call_count == 7
        mock_session.commit.assert_called_once()

    def test_no_store_when_no_positions(self):
        mock_session = self._make_mock_session()

        with (
            patch("collectors.shipping.settings") as mock_settings,
            patch("collectors.shipping.SessionLocal", return_value=mock_session),
        ):
            mock_settings.datalastic_api_key = ""
            from collectors.shipping import collect_and_store

            collect_and_store()

        mock_session.add.assert_not_called()

    def test_skips_fetch_when_no_api_key(self):
        with (
            patch("collectors.shipping.settings") as mock_settings,
            patch("collectors.shipping.requests.get") as mock_get,
        ):
            mock_settings.datalastic_api_key = ""
            from collectors.shipping import fetch_tanker_positions

            result = fetch_tanker_positions()

        assert result == []
        mock_get.assert_not_called()
