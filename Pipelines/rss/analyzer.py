"""
Article analysis: sentiment scoring and entity extraction.

Defines the ArticleAnalyzer protocol so any implementation can be dropped in.
Use build_analyzer() to instantiate the right one from config.

To add a new implementation:
  1. Create a class with an analyze(article) -> ArticleSentiment method.
  2. Add an elif branch in build_analyzer() keyed to a new `analyzer:` value in rss_feeds.yaml.
"""

import os
import re
import time
from typing import Protocol, runtime_checkable

from langchain_openai import ChatOpenAI

from .models import ArticleSentiment
from .rss_fetcher import Article


@runtime_checkable
class ArticleAnalyzer(Protocol):
    def analyze(self, article: Article) -> ArticleSentiment:
        """Analyze a single article, returning sentiment and mentioned entities."""
        ...


class LLMArticleAnalyzer:
    """
    Single-call LLM analyzer: returns sentiment, mentioned tickers, and mentioned
    sectors in one structured-output request.

    Configured via the `llm` block in rss_feeds.yaml:
      analyzer: llm
      base_url: ...
      model: ...
      entity_extraction:
        enabled: true
        include_companies: true   # include ticker+name list in prompt
        include_sectors: true     # include sector list in prompt
    """

    def __init__(
        self,
        llm_cfg: dict,
        companies: list[dict],
        sectors: list[str],
    ) -> None:
        self._extraction_cfg: dict = llm_cfg.get("entity_extraction", {})
        self._companies = companies
        self._sectors = sectors
        self._max_retries: int = llm_cfg.get("max_retries", 2)
        self._retry_delay: float = llm_cfg.get("retry_delay", 5)

        keep_alive = llm_cfg.get("keep_alive", None)
        model_kwargs = {"keep_alive": keep_alive} if keep_alive is not None else {}

        llm = ChatOpenAI(
            base_url=llm_cfg.get("base_url", "https://openrouter.ai/api/v1"),
            api_key=os.getenv("OPENROUTER_API_KEY"),
            model=llm_cfg.get("model", "openrouter/auto"),
            temperature=0,
            request_timeout=llm_cfg.get("request_timeout", 60),
            model_kwargs=model_kwargs,
        )
        self._llm = llm.with_structured_output(ArticleSentiment)

    def _build_prompt(self, article: Article) -> str:
        lang_hint = {"fr": "French", "ar": "Arabic", "en": "English"}.get(
            article.language, article.language
        )
        extraction_enabled = self._extraction_cfg.get("enabled", True)

        company_block = ""
        if extraction_enabled and self._extraction_cfg.get("include_companies", True):
            lines = []
            for c in self._companies:
                ticker = c.get("ticker", "").strip()
                name = c.get("libelle", "").strip()
                if ticker:
                    lines.append(f"  {ticker} — {name}")
            if lines:
                company_block = "Known BVC companies (ticker — name):\n" + "\n".join(lines) + "\n\n"

        sector_block = ""
        if extraction_enabled and self._extraction_cfg.get("include_sectors", True):
            unique = sorted({
                re.sub(r"^MASI\s+", "", c.get("secteur", "")).strip()
                for c in self._companies
                if c.get("secteur", "").strip()
            })
            if unique:
                sector_block = "Known BVC sectors:\n" + "\n".join(f"  {s}" for s in unique) + "\n\n"

        entity_instruction = ""
        if extraction_enabled:
            entity_instruction = (
                "Also identify any BVC companies and sectors from the provided lists "
                "that are explicitly or implicitly mentioned in the article. "
                "Return only values that appear in the lists above.\n\n"
            )

        return (
            f"You are a financial news analyst specializing in the Casablanca Stock Exchange (BVC/MASI).\n\n"
            f"Analyze the following {lang_hint}-language article for financial sentiment as it relates "
            f"to Moroccan listed companies or the BVC market overall. "
            f"Do not translate the article — analyze it in its original language.\n\n"
            f"{company_block}"
            f"{sector_block}"
            f"{entity_instruction}"
            f"Title: {article.title}\n\n"
            f"Content: {article.full_text}\n\n"
            f"Provide your structured analysis."
        )

    def analyze(self, article: Article) -> ArticleSentiment:
        prompt = self._build_prompt(article)
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                return self._llm.invoke(prompt)
            except Exception as e:
                last_exc = e
                if attempt < self._max_retries:
                    delay = self._retry_delay * (2 ** attempt)
                    print(f"  [RETRY] Attempt {attempt + 1} failed ({e}), retrying in {delay:.0f}s...")
                    time.sleep(delay)
        raise last_exc


def build_analyzer(
    llm_cfg: dict,
    companies: list[dict],
    sectors: list[str],
) -> ArticleAnalyzer:
    """
    Instantiate the configured ArticleAnalyzer implementation.
    Reads `llm.analyzer` from config (default: "llm").
    """
    name = llm_cfg.get("analyzer", "llm")

    if name == "llm":
        return LLMArticleAnalyzer(llm_cfg, companies, sectors)

    raise ValueError(
        f"Unknown analyzer '{name}'. "
        f"Set llm.analyzer in rss_feeds.yaml to a supported value (e.g. 'llm')."
    )
