"""
LLM-based sentiment analysis for RSS articles.

Uses the same OpenRouter/ChatOpenAI setup as agents/agent.py,
with Pydantic structured output for consistent, typed results.
"""

import os
from typing import Literal

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from .rss_fetcher import Article


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
        description="BVC ticker symbols explicitly or implicitly mentioned in the article",
    )


def build_sentiment_llm(llm_cfg: dict):
    """
    Instantiate ChatOpenAI exactly as in agents/agent.py, bound to ArticleSentiment
    structured output.
    """
    llm = ChatOpenAI(
        base_url=llm_cfg.get("base_url", "https://openrouter.ai/api/v1"),
        api_key=os.getenv("OPENROUTER_API_KEY"),
        model=llm_cfg.get("model", "openrouter/auto"),
        temperature=0,
    )
    return llm.with_structured_output(ArticleSentiment)


def build_sentiment_prompt(article: Article, known_tickers: list[str]) -> str:
    tickers_str = ", ".join(known_tickers) if known_tickers else "none provided"
    lang_hint = {
        "fr": "French",
        "ar": "Arabic",
        "en": "English",
    }.get(article.language, article.language)

    return (
        f"You are a financial news analyst specializing in the Casablanca Stock Exchange (BVC/MASI).\n\n"
        f"Analyze the following {lang_hint}-language article for financial sentiment as it relates "
        f"to Moroccan listed companies or the BVC market overall. "
        f"Do not translate the article — analyze it in its original language.\n\n"
        f"Title: {article.title}\n\n"
        f"Content: {article.full_text}\n\n"
        f"Known BVC tickers to watch for: {tickers_str}\n\n"
        f"Provide your structured sentiment analysis."
    )


def analyze_batch(
    sentiment_llm,
    articles: list[Article],
    known_tickers: list[str],
    already_done_urls: set[str],
) -> list[tuple[Article, ArticleSentiment]]:
    """
    Analyze a list of articles, skipping any already in already_done_urls.
    Returns (Article, ArticleSentiment) pairs for successfully analyzed articles.
    Logs a warning and skips on per-article failure.
    """
    results = []
    for article in articles:
        if article.url in already_done_urls:
            continue
        prompt = build_sentiment_prompt(article, known_tickers)
        try:
            sentiment: ArticleSentiment = sentiment_llm.invoke(prompt)
            results.append((article, sentiment))
        except Exception as e:
            print(f"  [WARN] Sentiment analysis failed for '{article.title[:60]}': {e}")
    return results
