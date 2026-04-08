"""Trading positions tracked from AI recommendations.

Every BUY/SELL recommendation issued by the AI brain creates an open Position
row. The analyzer / ai-brain checks open positions on each scoring cycle:
  - If price has hit TP → mark as closed_tp
  - If price has hit SL → mark as closed_sl
  - Otherwise the open positions are passed back to Opus so it can decide
    whether to HOLD, CLOSE, or ADD to each one.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from shared.models.base import Base


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # When the signal was generated
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    # When the position was closed (None while still open)
    closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # LONG | SHORT
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    # open | closed_tp | closed_sl | closed_manual | closed_strategy
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open", index=True)

    # Price levels
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    stop_loss: Mapped[float | None] = mapped_column(Float, nullable=True)
    take_profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Realised P/L in dollars (close - entry, signed by side). Computed on close.
    realised_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Link back to the recommendation that opened this position
    recommendation_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Free-form notes from Opus when closing / managing
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
