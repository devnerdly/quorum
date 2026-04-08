"""Binance USD-M Futures WebSocket streamer for live kline updates.

Subscribes to wss://fstream.binance.com/ws/<symbol>@kline_1m and upserts
the current (unfinished) 1-minute bar on every tick. Binance streams this
~1-2 messages per second while the market is open.

The stream format (per Binance docs):
{
  "e": "kline",
  "E": 123456789,
  "s": "CLUSDT",
  "k": {
    "t": 123400000,  // kline start time (ms)
    "T": 123459999,  // kline close time (ms)
    "s": "CLUSDT",
    "i": "1m",
    "f": 100,
    "L": 200,
    "o": "96.50",
    "c": "96.52",
    "h": "96.55",
    "l": "96.48",
    "v": "1000",
    "n": 100,
    "x": false,      // is this kline closed?
    "q": "100000",
    "V": "500",
    "Q": "50000",
    "B": "0"
  }
}

We upsert on every tick regardless of whether the bar closed — the current
minute bar is refreshed each update, and when it closes we move on to the
next minute's bar naturally. Public market data endpoints do not require
authentication.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared.config import settings
from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV
from shared.redis_streams import publish
from shared.schemas.events import PriceEvent

logger = logging.getLogger(__name__)

_WS_BASE = "wss://fstream.binance.com/ws"
_STREAM = "prices.brent"  # legacy stream name, carries CLUSDT data now
_SOURCE = "binance"


def _symbol() -> str:
    return (settings.binance_symbol or "CLUSDT").upper()


def _stream_url() -> str:
    return f"{_WS_BASE}/{_symbol().lower()}@kline_1m"


def _upsert_kline(kline: dict) -> None:
    """Upsert one 1m bar from a WS kline message into the DB and fan-out."""
    try:
        ts = datetime.fromtimestamp(kline["t"] / 1000, tz=timezone.utc)
        record = {
            "timestamp": ts,
            "source": _SOURCE,
            "timeframe": "1min",
            "open": float(kline["o"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
            "volume": float(kline.get("v") or 0.0),
        }
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Malformed kline payload: %s (%s)", kline, exc)
        return

    with SessionLocal() as session:
        stmt = pg_insert(OHLCV).values([record])
        stmt = stmt.on_conflict_do_update(
            index_elements=["source", "timeframe", "timestamp"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
            },
        )
        session.execute(stmt)
        session.commit()

    # Fan out to Redis so analyzer/live_watch react immediately. Only publish
    # on kline close ("x": True) to avoid flooding downstream with 1-2 msg/s.
    if kline.get("x") is True:
        event = PriceEvent(**record)
        publish(_STREAM, event.model_dump())
        logger.info(
            "WS kline closed: %s %s O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f",
            _symbol(), ts.strftime("%H:%M"),
            record["open"], record["high"], record["low"],
            record["close"], record["volume"],
        )


def _run_forever() -> None:
    """Reconnect loop for the kline stream. Runs in a daemon thread."""
    # Lazy import — the websocket-client lib is optional and only needed here.
    try:
        import websocket  # type: ignore
    except ImportError:
        logger.error(
            "websocket-client not installed — Binance WS disabled. "
            "Add 'websocket-client>=1.7' to data-collector requirements."
        )
        return

    backoff = 1.0
    while True:
        url = _stream_url()
        logger.info("Binance WS connecting: %s", url)
        try:
            ws = websocket.WebSocket()
            ws.connect(url, timeout=15)
            backoff = 1.0
            logger.info("Binance WS connected to %s", _symbol())

            while True:
                try:
                    raw = ws.recv()
                except Exception:
                    logger.exception("Binance WS recv() failed, reconnecting")
                    break
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("Non-JSON WS frame, skipping: %r", raw[:200])
                    continue
                if msg.get("e") == "kline" and isinstance(msg.get("k"), dict):
                    try:
                        _upsert_kline(msg["k"])
                    except Exception:
                        logger.exception("Failed to upsert kline from WS")
        except Exception:
            logger.exception("Binance WS connect/loop crashed")
        finally:
            try:
                ws.close()
            except Exception:
                pass

        logger.info("Binance WS reconnecting in %.1fs …", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60.0)


_WORKER_THREAD: threading.Thread | None = None


def start_binance_ws() -> None:
    """Launch the reconnecting WebSocket worker as a daemon thread.

    Idempotent — a second call is a no-op if the thread is already running.
    """
    global _WORKER_THREAD
    if _WORKER_THREAD is not None and _WORKER_THREAD.is_alive():
        logger.debug("Binance WS worker already running")
        return
    _WORKER_THREAD = threading.Thread(
        target=_run_forever,
        daemon=True,
        name="binance-ws",
    )
    _WORKER_THREAD.start()
    logger.info("Started Binance WS worker thread")
