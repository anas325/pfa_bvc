"""
Shared data models for the RSS sentiment pipeline.
"""

from typing import Literal

from pydantic import BaseModel, Field


class ArticleSentiment(BaseModel):
    sentiment: Literal["positive", "negative", "neutral"]
    score: float = Field(
        description="Sentiment score from -1.0 (very negative) to 1.0 (very positive)",
        ge=-1.0,
        le=1.0,
    )
    confidence: float = Field(
        description="Confidence in the sentiment assessment, 0.0 to 1.0",
        ge=0.0,
        le=1.0,
    )
    reasoning: str = Field(
        description="One or two sentence summary of why this sentiment was assigned"
    )
    mentioned_tickers: list[str] = Field(
        default_factory=list,
        description=(
            "BVC ticker symbols mentioned in the article. "
            "Only include values from the provided company list."
        ),
    )
    mentioned_sectors: list[str] = Field(
        default_factory=list,
        description=(
            "BVC market sectors mentioned in the article. "
            "Only include values from the provided sector list."
        ),
    )
