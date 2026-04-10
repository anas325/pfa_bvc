"""
Shared data models for the RSS sentiment pipeline.
"""

from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Sentiment values the LLM is known to produce that map to a canonical label.
_SENTIMENT_MAP: dict[str, str] = {
    "positive":       "positive",
    "positif":        "positive",
    "négatif":        "negative",
    "negatif":        "negative",
    "negative":       "negative",
    "neutral":        "neutral",
    "neutre":         "neutral",
    "mixed":          "neutral",
    "mitigé":         "neutral",
    "mitige":         "neutral",
    "neutre-positif": "positive",
    "neutre‑positif": "positive",
    "neutre-négatif": "negative",
    "neutre‑négatif": "negative",
}


class ArticleSentiment(BaseModel):
    sentiment: Literal["positive", "negative", "neutral"]
    score: float = Field(
        default=0.0,
        description="Sentiment score from -1.0 (very negative) to 1.0 (very positive)",
        ge=-1.0,
        le=1.0,
    )
    confidence: float = Field(
        default=0.0,
        description="Confidence in the sentiment assessment, 0.0 to 1.0",
        ge=0.0,
        le=1.0,
    )
    reasoning: str = Field(
        default="",
        description="One or two sentence summary of why this sentiment was assigned",
    )
    mentioned_tickers: list[str] = Field(
        default_factory=list,
        description=(
            "BVC company names or tickers mentioned in the article. "
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

    @field_validator("sentiment", mode="before")
    @classmethod
    def normalise_sentiment(cls, v: str) -> str:
        normalised = _SENTIMENT_MAP.get(str(v).lower().strip())
        if normalised:
            return normalised
        # Fall back: if the value contains "pos" lean positive, "neg" lean negative
        v_lower = str(v).lower()
        if "pos" in v_lower:
            return "positive"
        if "neg" in v_lower or "nég" in v_lower:
            return "negative"
        return "neutral"
