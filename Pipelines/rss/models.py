"""
Shared data models for the RSS sentiment pipeline.
"""

import hashlib
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Sentiment normalisation
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Event type taxonomy
# ---------------------------------------------------------------------------

EventType = Literal[
    "earnings_release",
    "dividend_announcement",
    "capital_operation",
    "debt_issuance",
    "ma_deal",
    "leadership_change",
    "regulatory_action",
    "strategic_plan",
    "market_data",
    "economic_indicator",
    "project_contract",
    "ipo_listing",
    "other",
]

# French/Arabic aliases the LLM might produce → canonical EventType value.
_EVENT_TYPE_MAP: dict[str, str] = {
    "résultats":      "earnings_release",
    "dividende":      "dividend_announcement",
    "augmentation":   "capital_operation",
    "obligataire":    "debt_issuance",
    "acquisition":    "ma_deal",
    "fusion":         "ma_deal",
    "cession":        "ma_deal",
    "nomination":     "leadership_change",
    "démission":      "leadership_change",
    "réglementation": "regulatory_action",
    "ammc":           "regulatory_action",
    "bkam":           "regulatory_action",
    "stratégie":      "strategic_plan",
    "indice":         "market_data",
    "masi":           "market_data",
    "indicateur":     "economic_indicator",
    "contrat":        "project_contract",
    "marché":         "project_contract",
    "introduction":   "ipo_listing",
}

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class PersonMention(BaseModel):
    name: str = Field(description="Full name as it appears in the article.")
    role: str = Field(
        default="",
        description="Title or role (e.g. PDG, Directeur Général, Ministre). Empty if unknown.",
    )

    @property
    def normalized_name(self) -> str:
        """ASCII-folded, lowercased name used as the Neo4j unique key."""
        import unicodedata
        nfkd = unicodedata.normalize("NFKD", self.name)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


# ---------------------------------------------------------------------------
# Main analysis model
# ---------------------------------------------------------------------------


class ArticleSentiment(BaseModel):
    # ---- Sentiment (original fields) ----
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

    # ---- Event extraction (new fields) ----
    event_type: EventType = Field(
        default="other",
        description="The primary type of financial event described in this article.",
    )
    event_date: Optional[str] = Field(
        default=None,
        description=(
            "ISO 8601 date (YYYY-MM-DD) of the event described, if explicitly stated. "
            "Null if no specific date is mentioned."
        ),
    )
    mentioned_people: list[PersonMention] = Field(
        default_factory=list,
        description=(
            "Named individuals (executives, ministers, regulators) mentioned in the article "
            "who are directly relevant to the financial event. Maximum 5 people."
        ),
    )

    # Set by Python post-LLM — not an LLM output.
    event_fingerprint: str = Field(
        default="",
        description="Deduplication key computed from event_type + tickers + date. Leave empty.",
    )

    @field_validator("sentiment", mode="before")
    @classmethod
    def normalise_sentiment(cls, v: str) -> str:
        normalised = _SENTIMENT_MAP.get(str(v).lower().strip())
        if normalised:
            return normalised
        v_lower = str(v).lower()
        if "pos" in v_lower:
            return "positive"
        if "neg" in v_lower or "nég" in v_lower:
            return "negative"
        return "neutral"

    @field_validator("event_type", mode="before")
    @classmethod
    def normalise_event_type(cls, v: str) -> str:
        v_lower = str(v).lower().strip()
        # Direct match against the Literal values
        valid = {
            "earnings_release", "dividend_announcement", "capital_operation",
            "debt_issuance", "ma_deal", "leadership_change", "regulatory_action",
            "strategic_plan", "market_data", "economic_indicator",
            "project_contract", "ipo_listing", "other",
        }
        if v_lower in valid:
            return v_lower
        # French/alias fallback
        for key, canonical in _EVENT_TYPE_MAP.items():
            if key in v_lower:
                return canonical
        return "other"


# ---------------------------------------------------------------------------
# Fingerprint computation (pure Python, zero LLM cost)
# ---------------------------------------------------------------------------


def compute_fingerprint(analysis: ArticleSentiment, published_at: datetime) -> str:
    """
    Deterministic deduplication key for an event.

    Groups articles that share the same event_type, involved tickers, and
    publication/event ISO week into a single 16-char hex string.
    Returns "" for generic ("other") articles — no Event node is created for those.
    """
    if analysis.event_type == "other":
        return ""

    if analysis.event_date:
        try:
            year, week, _ = datetime.fromisoformat(analysis.event_date).isocalendar()
        except ValueError:
            year, week, _ = published_at.isocalendar()
    else:
        year, week, _ = published_at.isocalendar()

    date_bucket = f"{year}-W{week:02d}"
    tickers_key = "|".join(sorted(set(analysis.mentioned_tickers)))
    raw = f"{analysis.event_type}::{tickers_key}::{date_bucket}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
