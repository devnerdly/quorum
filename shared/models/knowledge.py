"""Knowledge summaries — periodic Haiku-generated digests of @marketfeed news.

Each row is a 5-minute summary of breaking-news messages from a given source
(currently @marketfeed) so the system has a queryable knowledge base of what
was happening at any point in time. Opus reads the most recent N summaries on
every cycle to anchor its analysis in the latest news flow.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from shared.models.base import Base


class KnowledgeSummary(Base):
    __tablename__ = "knowledge_summaries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    # @marketfeed | rss | twitter | combined
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    # Time window the summary covers (e.g. "5min", "1h", "1d")
    window: Mapped[str] = mapped_column(String(16), nullable=False, default="5min")

    # Number of source messages aggregated into this summary
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Haiku-generated paragraph summary
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    # Bullet-point list of the most material individual events (JSON array)
    key_events: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Aggregate sentiment score for the window (-1.0 … +1.0)
    sentiment_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    # bullish | bearish | neutral
    sentiment_label: Mapped[str | None] = mapped_column(String(16), nullable=True)
