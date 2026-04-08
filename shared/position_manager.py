"""Position lifecycle helpers shared across services.

Used by ai-brain (to open positions from new signals & check TP/SL),
notifier (to format position alerts), and dashboard (to display state).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select

from shared.models.base import SessionLocal
from shared.models.ohlcv import OHLCV
from shared.models.positions import Position

logger = logging.getLogger(__name__)


def get_current_price() -> float | None:
    """Return the most recent Brent close (Stooq ICE preferred, Yahoo fallback)."""
    with SessionLocal() as session:
        row = (
            session.query(OHLCV)
            .filter(OHLCV.timeframe == "1min", OHLCV.source == "stooq")
            .order_by(OHLCV.timestamp.desc())
            .first()
        )
        if row is None:
            row = (
                session.query(OHLCV)
                .filter(OHLCV.timeframe == "1min")
                .order_by(OHLCV.timestamp.desc())
                .first()
            )
        return float(row.close) if row else None


def get_current_bar() -> tuple[float, float, float] | None:
    """Return (high, low, close) of the latest Yahoo 1-min bar."""
    with SessionLocal() as session:
        row = (
            session.query(OHLCV)
            .filter(OHLCV.timeframe == "1min", OHLCV.source == "yahoo")
            .order_by(OHLCV.timestamp.desc())
            .first()
        )
        return (float(row.high), float(row.low), float(row.close)) if row else None


def list_open_positions() -> list[dict]:
    """Return a list of currently-open positions enriched with live P/L."""
    price = get_current_price()
    with SessionLocal() as session:
        stmt = select(Position).where(Position.status == "open").order_by(Position.opened_at)
        rows = session.scalars(stmt).all()

        result: list[dict] = []
        for p in rows:
            unrealised = None
            if price is not None:
                if p.side == "LONG":
                    unrealised = price - p.entry_price
                elif p.side == "SHORT":
                    unrealised = p.entry_price - price

            result.append(
                {
                    "id": p.id,
                    "side": p.side,
                    "status": p.status,
                    "opened_at": p.opened_at.isoformat() if p.opened_at else None,
                    "entry_price": p.entry_price,
                    "stop_loss": p.stop_loss,
                    "take_profit": p.take_profit,
                    "current_price": price,
                    "unrealised_pnl": round(unrealised, 4) if unrealised is not None else None,
                    "unrealised_pct": (
                        round((unrealised / p.entry_price) * 100, 2)
                        if unrealised is not None
                        else None
                    ),
                    "recommendation_id": p.recommendation_id,
                }
            )
    return result


def open_position(
    side: str,
    entry_price: float,
    stop_loss: float | None,
    take_profit: float | None,
    recommendation_id: int | None = None,
    notes: str | None = None,
) -> int | None:
    """Insert a new open Position and return its id."""
    side_norm = side.upper()
    if side_norm not in ("LONG", "SHORT"):
        return None

    with SessionLocal() as session:
        row = Position(
            opened_at=datetime.now(tz=timezone.utc),
            side=side_norm,
            status="open",
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            recommendation_id=recommendation_id,
            notes=notes,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        logger.info(
            "Opened position #%s %s @ %.2f SL=%s TP=%s",
            row.id, side_norm, entry_price, stop_loss, take_profit,
        )
        return row.id


def close_position(
    position_id: int,
    close_price: float,
    status: str,
    notes: str | None = None,
) -> dict | None:
    """Close a position and return the closed-row snapshot."""
    with SessionLocal() as session:
        # Atomic claim: only succeeds if status is still 'open'.
        # WITH FOR UPDATE SKIP LOCKED prevents double-close race conditions.
        row = session.execute(
            select(Position)
            .where(Position.id == position_id, Position.status == "open")
            .with_for_update(skip_locked=True)
        ).scalar_one_or_none()
        if row is None:
            return None  # already closed by someone else

        if row.side == "LONG":
            pnl = close_price - row.entry_price
        else:  # SHORT
            pnl = row.entry_price - close_price

        row.status = status
        row.close_price = close_price
        row.closed_at = datetime.now(tz=timezone.utc)
        row.realised_pnl = pnl
        if notes:
            row.notes = (row.notes + "\n" if row.notes else "") + notes
        session.commit()

        logger.info(
            "Closed position #%s %s status=%s pnl=%+.2f",
            row.id, row.side, status, pnl,
        )
        return {
            "id": row.id,
            "side": row.side,
            "status": status,
            "entry_price": row.entry_price,
            "close_price": close_price,
            "realised_pnl": pnl,
            "stop_loss": row.stop_loss,
            "take_profit": row.take_profit,
        }


def check_tp_sl_hits() -> list[dict]:
    """Scan open positions and close any whose TP / SL has been hit.

    Returns the list of newly-closed position snapshots so the caller can
    notify the user.
    """
    bar = get_current_bar()
    if bar is None:
        return []
    high, low, close = bar

    closed: list[dict] = []
    with SessionLocal() as session:
        stmt = select(Position).where(Position.status == "open")
        for p in session.scalars(stmt).all():
            hit = None
            close_at: float = close
            if p.side == "LONG":
                if p.stop_loss is not None and low <= p.stop_loss:
                    hit = "closed_sl"
                    close_at = p.stop_loss  # exit at level, not the wick extreme
                elif p.take_profit is not None and high >= p.take_profit:
                    hit = "closed_tp"
                    close_at = p.take_profit
            elif p.side == "SHORT":
                if p.stop_loss is not None and high >= p.stop_loss:
                    hit = "closed_sl"
                    close_at = p.stop_loss
                elif p.take_profit is not None and low <= p.take_profit:
                    hit = "closed_tp"
                    close_at = p.take_profit

            if hit:
                snap = close_position(
                    p.id,
                    close_price=close_at,
                    status=hit,
                    notes=f"Auto-closed at {close_at:.2f} ({hit}, bar high={high:.2f} low={low:.2f})",
                )
                if snap:
                    closed.append(snap)

    return closed
