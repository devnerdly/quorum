"""Tests for the Yahoo Finance collector."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame that mimics yf.download output."""
    index = pd.DatetimeIndex(
        [r["ts"] for r in rows], tz="UTC", name="Datetime"
    )
    data = {
        "Open":   [r["o"] for r in rows],
        "High":   [r["h"] for r in rows],
        "Low":    [r["l"] for r in rows],
        "Close":  [r["c"] for r in rows],
        "Volume": [r["v"] for r in rows],
    }
    return pd.DataFrame(data, index=index)


_SAMPLE_ROWS = [
    {"ts": "2024-01-02 09:00:00", "o": 77.1, "h": 78.0, "l": 76.5, "c": 77.8, "v": 10000},
    {"ts": "2024-01-02 10:00:00", "o": 77.8, "h": 79.2, "l": 77.5, "c": 78.9, "v": 12000},
]


# ---------------------------------------------------------------------------
# fetch_brent_ohlcv
# ---------------------------------------------------------------------------

class TestFetchBrentOhlcv:
    def test_returns_correct_shape(self):
        df = _make_df(_SAMPLE_ROWS)
        with patch("yfinance.download", return_value=df) as mock_dl:
            from collectors.yahoo import fetch_brent_ohlcv

            result = fetch_brent_ohlcv(interval="1h", period="1d")

        mock_dl.assert_called_once_with(
            "BZ=F",
            interval="1h",
            period="1d",
            progress=False,
            auto_adjust=True,
        )
        assert len(result) == 2

    def test_record_fields(self):
        df = _make_df(_SAMPLE_ROWS)
        with patch("yfinance.download", return_value=df):
            from collectors.yahoo import fetch_brent_ohlcv

            records = fetch_brent_ohlcv(interval="1h", period="1d")

        rec = records[0]
        assert set(rec.keys()) == {"timestamp", "source", "timeframe", "open", "high", "low", "close", "volume"}
        assert rec["source"] == "yahoo"
        assert rec["timeframe"] == "1H"
        assert isinstance(rec["timestamp"], datetime)
        assert rec["timestamp"].tzinfo is not None  # must be tz-aware
        assert rec["open"] == pytest.approx(77.1)
        assert rec["high"] == pytest.approx(78.0)
        assert rec["low"] == pytest.approx(76.5)
        assert rec["close"] == pytest.approx(77.8)
        assert rec["volume"] == pytest.approx(10000.0)

    def test_interval_map_applied(self):
        df = _make_df(_SAMPLE_ROWS)
        with patch("yfinance.download", return_value=df):
            from collectors.yahoo import fetch_brent_ohlcv

            records = fetch_brent_ohlcv(interval="1d", period="1mo")

        assert records[0]["timeframe"] == "1D"

    def test_empty_dataframe_returns_empty_list(self):
        with patch("yfinance.download", return_value=pd.DataFrame()):
            from collectors.yahoo import fetch_brent_ohlcv

            result = fetch_brent_ohlcv(interval="1h", period="1d")

        assert result == []


# ---------------------------------------------------------------------------
# collect_and_store
# ---------------------------------------------------------------------------

class TestCollectAndStore:
    def test_stores_to_db_and_publishes(self):
        df = _make_df(_SAMPLE_ROWS)

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with (
            patch("yfinance.download", return_value=df),
            patch("collectors.yahoo.SessionLocal", return_value=mock_session),
            patch("collectors.yahoo.publish") as mock_publish,
        ):
            from collectors.yahoo import collect_and_store

            collect_and_store(interval="1h", period="1d")

        # DB: add should be called for each row
        assert mock_session.add.call_count == len(_SAMPLE_ROWS)
        mock_session.commit.assert_called_once()

        # Redis: publish called once with the latest bar
        mock_publish.assert_called_once()
        stream_arg, data_arg = mock_publish.call_args.args
        assert stream_arg == "prices.brent"
        assert data_arg["source"] == "yahoo"
        assert data_arg["timeframe"] == "1H"

    def test_no_publish_when_empty(self):
        with (
            patch("yfinance.download", return_value=pd.DataFrame()),
            patch("collectors.yahoo.publish") as mock_publish,
        ):
            from collectors.yahoo import collect_and_store

            collect_and_store(interval="1h", period="1d")

        mock_publish.assert_not_called()
