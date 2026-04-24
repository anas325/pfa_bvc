"""
RSS sentiment pipeline entry point.

Two-phase pipeline:
  Phase 1 — Fetch RSS feeds and store raw articles in Neo4j (resumable).
  Phase 2 — Run LLM analysis on stored articles that have no sentiment yet (resumable).

Usage:
    cd Pipelines
    uv run python -m rss.pipeline
"""

import csv
import os
import re
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ROOT = Path(__file__).parent.parent
CONFIG_FILE = ROOT / "config" / "rss_feeds.yaml"
COMPANIES_CSV = ROOT / "data" / "companies.csv"

from db.db import get_driver
from monitoring import PipelineLogger
from rss.analyzer import build_analyzer
from rss.rss_fetcher import fetch_all_feeds
from rss.neo4j_loader import (
    ensure_constraints,
    get_stored_article_urls,
    get_unanalyzed_articles,
    seed_companies,
    seed_company_sectors,
    seed_sectors,
    store_articles_raw,
    store_sentiment_batch,
)


def load_config() -> dict:
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_companies() -> list[dict]:
    with open(COMPANIES_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def extract_sectors(companies: list[dict]) -> list[str]:
    return sorted({
        re.sub(r"^MASI\s+", "", c.get("secteur", "")).strip()
        for c in companies
        if c.get("secteur", "").strip()
    })


def main() -> None:
    config = load_config()
    companies = load_companies()
    sectors = extract_sectors(companies)
    llm_cfg = config.get("llm", {})
    batch_size = llm_cfg.get("batch_size", 5)
    inter_article_delay: float = llm_cfg.get("inter_article_delay", 0)

    print("=== BVC RSS Sentiment Pipeline ===\n")

    with PipelineLogger("rss_pipeline") as log:
        driver = get_driver()
        driver.verify_connectivity()

        print("Setting up Neo4j schema...")
        ensure_constraints(driver)
        seed_companies(driver, companies)
        seed_sectors(driver, companies)
        seed_company_sectors(driver, companies)
        print(f"  Seeded {len(companies)} companies, {len(sectors)} sectors.\n")
        log.event(
            f"seeded {len(companies)} companies, {len(sectors)} sectors",
            stage="seed",
        )

        # --- Phase 1: Fetch and store raw articles ---
        print("Phase 1 — Fetching RSS feeds...")
        log.event("fetching RSS feeds", stage="fetch")
        articles, failed_feeds = fetch_all_feeds(config)
        print()

        for name in failed_feeds:
            log.event(f"feed failed: {name}", level="warning", stage="fetch")
        if failed_feeds:
            log.metric("feeds_failed", len(failed_feeds), stage="fetch")

        enabled_count = sum(1 for f in config.get("feeds", []) if f.get("enabled", True))
        if len(failed_feeds) == enabled_count:
            raise RuntimeError(
                f"All {enabled_count} enabled RSS feeds failed — aborting pipeline."
            )

        log.metric("articles_fetched", len(articles), stage="fetch")

        stored_urls = get_stored_article_urls(driver)
        new_articles = [a for a in articles if a.url not in stored_urls]
        print(f"  {len(stored_urls)} already stored, {len(new_articles)} new to save.\n")
        log.event(
            f"{len(stored_urls)} already stored, {len(new_articles)} new",
            stage="fetch",
        )

        if new_articles:
            store_articles_raw(driver, new_articles)
            log.metric("articles_stored", len(new_articles), stage="store")
            print(f"  Saved {len(new_articles)} articles.\n")

        # --- Phase 2: Analyze stored articles ---
        print("Phase 2 — Running LLM analysis...")
        to_analyze = get_unanalyzed_articles(driver)
        print(f"  {len(to_analyze)} articles pending analysis.\n")
        log.event(f"{len(to_analyze)} articles pending analysis", stage="analyze")

        if not to_analyze:
            print("Nothing to analyze. Done.")
            driver.close()
            return

        analyzer = build_analyzer(llm_cfg, companies, sectors)

        for i in tqdm(range(0, len(to_analyze), batch_size)):
            batch = to_analyze[i : i + batch_size]
            results = []
            batch_failed = 0

            for article in batch:
                try:
                    sentiment = analyzer.analyze(article)
                    results.append((article.url, sentiment))
                    log.increment_processed()
                    if inter_article_delay > 0:
                        time.sleep(inter_article_delay)
                except Exception as e:
                    print(f"  [WARN] Analysis failed for '{article.title[:60]}': {e}")
                    log.increment_failed()
                    log.event(
                        f"analysis failed for '{article.title[:80]}': {e}",
                        level="warning",
                        stage="analyze",
                        item_key=article.url,
                    )
                    batch_failed += 1

            store_sentiment_batch(driver, results)

            if batch_failed == len(batch):
                raise RuntimeError(
                    f"LLM analysis failed for all {batch_failed} articles in batch "
                    f"(starting at index {i}) — aborting pipeline."
                )

            completed = min(i + batch_size, len(to_analyze))
            print(f"  {completed}/{len(to_analyze)} articles analyzed and stored.")
            log.metric(
                "analyzed_progress",
                completed,
                stage="analyze",
                message=f"{completed}/{len(to_analyze)} analyzed",
            )

        driver.close()
        print("\nDone.")


if __name__ == "__main__":
    missing = [v for v in ("OPENROUTER_API_KEY", "NEO4J_PASSWORD") if not os.getenv(v)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    main()
