from datetime import datetime
from typing import Any

from pydantic import BaseModel


class PriceEvent(BaseModel):
    """Emitted when a new OHLCV bar is stored."""

    timestamp: datetime
    source: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


class MacroEvent(BaseModel):
    """Emitted when a macro data point is ingested."""

    timestamp: datetime
    dataset: str  # e.g. "eia", "cot", "fred", "jodi", "opec"
    data: dict[str, Any]


class SentimentEvent(BaseModel):
    """Emitted when a sentiment record is stored."""

    timestamp: datetime
    source_type: str  # "news" or "twitter"
    sentiment: str
    score: float
    relevance: float = 1.0
    summary: str | None = None


class ScoresEvent(BaseModel):
    """Emitted by the Analyzer with module and unified scores."""

    timestamp: datetime
    technical_score: float | None = None
    fundamental_score: float | None = None
    sentiment_score: float | None = None
    shipping_score: float | None = None
    unified_score: float | None = None


class RecommendationEvent(BaseModel):
    """Emitted by the AI Brain with the final trading recommendation."""

    timestamp: datetime
    action: str  # BUY / SELL / HOLD
    unified_score: float | None = None
    opus_override_score: float | None = None
    confidence: float | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    haiku_summary: str | None = None
    grok_narrative: str | None = None
