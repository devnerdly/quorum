"""Account manager — singleton account row helpers and state recomputation.

All mutations use SELECT FOR UPDATE to prevent race conditions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select

from shared.models.base import SessionLocal
from shared.models.account import Account
from shared.models.positions import Position

logger = logging.getLogger(__name__)

STARTING_BALANCE = 100_000.0
DEFAULT_LEVERAGE = 10


def get_or_create_account() -> Account:
    """Return the singleton Account row, creating it with defaults if missing."""
    with SessionLocal() as session:
        row = session.query(Account).first()
        if row is None:
            now = datetime.now(tz=timezone.utc)
            row = Account(
                starting_balance=STARTING_BALANCE,
                cash=STARTING_BALANCE,
                realized_pnl_total=0.0,
                leverage=DEFAULT_LEVERAGE,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            logger.info("Created new account row (starting_balance=%.2f)", STARTING_BALANCE)
        return row


def recompute_account_state() -> dict:
    """Compute and return the full account state dict matching the /api/account contract.

    This is a read-only computation — it does NOT mutate the account row.
    """
    from shared.position_manager import get_current_price

    current_price = get_current_price()

    with SessionLocal() as session:
        account = session.query(Account).first()
        if account is None:
            # Auto-bootstrap on first call
            account = _create_account_in_session(session)

        cash = account.cash
        realized_pnl_total = account.realized_pnl_total
        starting_balance = account.starting_balance
        leverage = account.leverage

        # Gather all open positions
        open_positions = (
            session.query(Position)
            .filter(Position.status == "open")
            .all()
        )

        total_margin_used = 0.0
        total_unrealised = 0.0
        open_campaign_ids: set[int] = set()

        for p in open_positions:
            margin = p.margin_used or 0.0
            total_margin_used += margin

            if p.campaign_id is not None:
                open_campaign_ids.add(p.campaign_id)

            # Compute unrealised PnL in USD
            if current_price is not None and p.lots is not None:
                lots = p.lots
                if p.side == "LONG":
                    pnl = (current_price - p.entry_price) * lots * 100
                else:  # SHORT
                    pnl = (p.entry_price - current_price) * lots * 100
                total_unrealised += pnl

        equity = cash + total_unrealised

        if total_margin_used > 0:
            margin_level_pct = round((equity / total_margin_used) * 100, 2)
        else:
            margin_level_pct = None  # infinite / no exposure

        free_margin = equity - total_margin_used

        return {
            "starting_balance": starting_balance,
            "cash": round(cash, 2),
            "equity": round(equity, 2),
            "margin_used": round(total_margin_used, 2),
            "free_margin": round(free_margin, 2),
            "margin_level_pct": margin_level_pct,
            "realized_pnl_total": round(realized_pnl_total, 2),
            "unrealised_pnl": round(total_unrealised, 2),
            "open_campaigns": len(open_campaign_ids),
            "leverage": leverage,
        }


def apply_position_open(margin_used: float) -> None:
    """Deduct margin from cash when a position is opened."""
    with SessionLocal() as session:
        account = (
            session.execute(
                select(Account).with_for_update()
            ).scalar_one_or_none()
        )
        if account is None:
            account = _create_account_in_session(session)
        account.cash -= margin_used
        account.updated_at = datetime.now(tz=timezone.utc)
        session.commit()
        logger.debug("apply_position_open: cash now %.2f (-%s margin)", account.cash, margin_used)


def apply_position_close(margin_used: float, realized_pnl: float) -> None:
    """Return margin to cash and book realized PnL when a position is closed."""
    with SessionLocal() as session:
        account = (
            session.execute(
                select(Account).with_for_update()
            ).scalar_one_or_none()
        )
        if account is None:
            account = _create_account_in_session(session)
        account.cash += margin_used + realized_pnl
        account.realized_pnl_total += realized_pnl
        account.updated_at = datetime.now(tz=timezone.utc)
        session.commit()
        logger.debug(
            "apply_position_close: cash now %.2f (+%.2f margin +%.2f pnl)",
            account.cash, margin_used, realized_pnl,
        )


def _create_account_in_session(session) -> Account:
    """Create and flush account row within an existing session (no commit)."""
    now = datetime.now(tz=timezone.utc)
    account = Account(
        starting_balance=STARTING_BALANCE,
        cash=STARTING_BALANCE,
        realized_pnl_total=0.0,
        leverage=DEFAULT_LEVERAGE,
        created_at=now,
        updated_at=now,
    )
    session.add(account)
    session.flush()
    return account
