from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, Text
from sqlalchemy.orm import mapped_column, Mapped

from shared.models.base import Base


class ShippingPosition(Base):
    """AIS vessel position snapshot for tankers of interest."""

    __tablename__ = "shipping_positions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    vessel_name: Mapped[str] = mapped_column(String(256), nullable=False)
    imo: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    vessel_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    lon: Mapped[float | None] = mapped_column(Float, nullable=True)
    speed: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    destination: Mapped[str | None] = mapped_column(String(256), nullable=True)
    eta: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ShippingMetric(Base):
    """Aggregated shipping metrics (e.g. fleet utilisation, congestion index)."""

    __tablename__ = "shipping_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    metric_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    # JSON-encoded extra details
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
