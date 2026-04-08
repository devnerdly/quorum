from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import mapped_column, Mapped

from shared.models.base import Base


class MacroEIA(Base):
    """U.S. Energy Information Administration weekly petroleum inventory data."""

    __tablename__ = "macro_eia"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    report_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Crude oil inventories (thousand barrels)
    crude_inventory_total: Mapped[float | None] = mapped_column(Float, nullable=True)
    crude_inventory_change: Mapped[float | None] = mapped_column(Float, nullable=True)
    # SPR
    spr_inventory: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Cushing hub inventories
    cushing_inventory: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Production (thousand barrels per day)
    crude_production: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Refinery utilization (%)
    refinery_utilization: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Imports / exports (thousand barrels per day)
    crude_imports: Mapped[float | None] = mapped_column(Float, nullable=True)
    crude_exports: Mapped[float | None] = mapped_column(Float, nullable=True)


class MacroCOT(Base):
    """CFTC Commitment of Traders report for crude oil futures."""

    __tablename__ = "macro_cot"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    report_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Commercial (hedgers)
    commercial_long: Mapped[float | None] = mapped_column(Float, nullable=True)
    commercial_short: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Non-commercial (speculators / large traders)
    non_commercial_long: Mapped[float | None] = mapped_column(Float, nullable=True)
    non_commercial_short: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Open interest
    open_interest: Mapped[float | None] = mapped_column(Float, nullable=True)


class MacroFRED(Base):
    """Federal Reserve Economic Data time series values."""

    __tablename__ = "macro_fred"
    __table_args__ = (
        UniqueConstraint("series_id", "timestamp", name="uq_macro_fred_series_ts"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    series_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)


class MacroJODI(Base):
    """Joint Organisations Data Initiative oil statistics."""

    __tablename__ = "macro_jodi"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    country: Mapped[str] = mapped_column(String(64), nullable=False)
    product: Mapped[str] = mapped_column(String(64), nullable=False)
    flow: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)


class MacroOPEC(Base):
    """OPEC monthly oil market report data."""

    __tablename__ = "macro_opec"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    report_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Production (million barrels per day)
    total_production: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Demand forecasts
    demand_forecast: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Supply forecasts (non-OPEC)
    supply_forecast: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Raw report text for AI parsing
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
