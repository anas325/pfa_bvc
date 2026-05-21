"""
Article analysis: sentiment scoring and entity extraction.

Defines the ArticleAnalyzer protocol so any implementation can be dropped in.
Use build_analyzer() to instantiate the right one from config.

To add a new implementation:
  1. Create a class with an analyze(article) -> ArticleSentiment method.
  2. Add an elif branch in build_analyzer() keyed to a new `analyzer:` value in rss_feeds.yaml.
"""

import json
import os
import re
import time
import urllib.request
from typing import Protocol, runtime_checkable

from langchain_openai import ChatOpenAI

from .models import ArticleSentiment, compute_fingerprint
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

        # Build Ollama unload URL if keep_alive is configured.
        # We call Ollama's native /api/generate endpoint directly after each invoke
        # because keep_alive cannot be passed through the OpenAI-compatible layer.
        self._ollama_unload_url: str | None = None
        if llm_cfg.get("keep_alive") is not None:
            base = llm_cfg.get("base_url", "").rstrip("/").removesuffix("/v1")
            self._ollama_model = llm_cfg.get("model", "")
            self._ollama_unload_url = f"{base}/api/generate"

        llm = ChatOpenAI(
            base_url=llm_cfg.get("base_url", "https://openrouter.ai/api/v1"),
            api_key=os.getenv("OPENROUTER_API_KEY"),
            model=llm_cfg.get("model", "openrouter/auto"),
            temperature=0,
            request_timeout=llm_cfg.get("request_timeout", 60),
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

        event_block = ""
        if extraction_enabled and self._extraction_cfg.get("include_events", True):
            event_block = (
                "Event classification:\n"
                "Identify the PRIMARY financial event type from exactly one of:\n"
                "  earnings_release       — financial results, revenue, profit, EBITDA\n"
                "  dividend_announcement  — dividend payment or yield announcement\n"
                "  capital_operation      — capital increase, share issuance, buyback\n"
                "  debt_issuance          — bond, sukuk, credit facility\n"
                "  ma_deal                — merger, acquisition, stake purchase, divestiture\n"
                "  leadership_change      — CEO/board appointment or resignation\n"
                "  regulatory_action      — AMMC/BKAM rulings, sanctions, authorizations\n"
                "  strategic_plan         — multi-year plans, new business lines\n"
                "  market_data            — index levels, trading volumes, market cap\n"
                "  economic_indicator     — GDP, inflation, interest rate, trade balance\n"
                "  project_contract       — public contracts, concessions, signed deals\n"
                "  ipo_listing            — IPO, new listing, OPV/OPR\n"
                "  other                  — if none of the above clearly applies\n"
                "If a specific date for the event is stated in the article, set event_date as YYYY-MM-DD. "
                "Otherwise leave event_date null.\n\n"
            )

        max_people = self._extraction_cfg.get("max_people", 5)
        people_block = ""
        if extraction_enabled and self._extraction_cfg.get("include_people", True):
            people_block = (
                f"Named people: List up to {max_people} named individuals (executives, ministers, "
                f"regulators) with their title/role. Only include those directly relevant to the "
                f"financial event described. Leave mentioned_people empty if no relevant individuals "
                f"are named.\n\n"
            )

        return (
            f"You are a financial news analyst specializing in the Casablanca Stock Exchange (BVC/MASI).\n\n"
            f"Analyze the following {lang_hint}-language article for financial sentiment as it relates "
            f"to Moroccan listed companies or the BVC market overall. "
            f"Do not translate the article — analyze it in its original language.\n\n"
            f"{company_block}"
            f"{sector_block}"
            f"{entity_instruction}"
            f"{event_block}"
            f"{people_block}"
            f"Title: {article.title}\n\n"
            f"Content: {article.full_text}\n\n"
            f"Provide your structured analysis."
        )

    def _unload_model(self) -> None:
        if not self._ollama_unload_url:
            return
        try:
            body = json.dumps({"model": self._ollama_model, "keep_alive": 0}).encode()
            req = urllib.request.Request(
                self._ollama_unload_url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception:
            pass

    def analyze(self, article: Article) -> ArticleSentiment:
        prompt = self._build_prompt(article)
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                result = self._llm.invoke(prompt)
                result.event_fingerprint = compute_fingerprint(result, article.published_at)
                self._unload_model()
                return result
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
