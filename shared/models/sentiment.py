from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, Text
from sqlalchemy.orm import mapped_column, Mapped

from shared.models.base import Base


class SentimentNews(Base):
    """News article sentiment scored for oil market relevance."""

    __tablename__ = "sentiment_news"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=True)
    # Sentiment label: bullish / bearish / neutral
    sentiment: Mapped[str] = mapped_column(String(16), nullable=False)
    # Sentiment score: -1.0 (very bearish) … +1.0 (very bullish)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    # Relevance to Brent crude: 0.0 … 1.0
    relevance: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)


class SentimentTwitter(Base):
    """Aggregated Twitter / X narrative sentiment for crude oil."""

    __tablename__ = "sentiment_twitter"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    # Dominant narrative detected (e.g. "supply cut optimism")
    narrative: Mapped[str] = mapped_column(Text, nullable=False)
    # Aggregate sentiment score: -1.0 … +1.0
    score: Mapped[float] = mapped_column(Float, nullable=False)
    # Comma-separated key topics / hashtags
    key_topics: Mapped[str | None] = mapped_column(Text, nullable=True)
