"""Account model — single-row table tracking the trading account state."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Integer
from sqlalchemy.orm import Mapped, mapped_column

from shared.models.base import Base


class Account(Base):
    """Single-row table tracking the trading account state."""
    __tablename__ = "account"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    starting_balance: Mapped[float] = mapped_column(Float, nullable=False, default=100000.0)
    cash: Mapped[float] = mapped_column(Float, nullable=False, default=100000.0)
    realized_pnl_total: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    leverage: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
