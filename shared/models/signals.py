from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, Text
from sqlalchemy.orm import mapped_column, Mapped

from shared.models.base import Base


class AnalysisScore(Base):
    """Composite scores produced by each analysis module."""

    __tablename__ = "analysis_scores"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    # Module scores: -1.0 (very bearish) … +1.0 (very bullish)
    technical_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    fundamental_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    sentiment_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    shipping_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Weighted combination of above scores
    unified_score: Mapped[float | None] = mapped_column(Float, nullable=True)


class AIRecommendation(Base):
    """Trading recommendation produced by the AI brain."""

    __tablename__ = "ai_recommendations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    # Input score from analyzer
    unified_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Opus override score (if model disagrees with unified)
    opus_override_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Model confidence: 0.0 … 1.0
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Trading action: BUY / SELL / HOLD
    action: Mapped[str] = mapped_column(String(8), nullable=False)
    # Full analysis narrative
    analysis_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Scenario narratives
    base_scenario: Mapped[str | None] = mapped_column(Text, nullable=True)
    alt_scenario: Mapped[str | None] = mapped_column(Text, nullable=True)
    # JSON-encoded risk factor list
    risk_factors: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Entry / stop-loss / take-profit prices
    entry_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    stop_loss: Mapped[float | None] = mapped_column(Float, nullable=True)
    take_profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Short summary from Haiku
    haiku_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Narrative from Grok / xAI
    grok_narrative: Mapped[str | None] = mapped_column(Text, nullable=True)
