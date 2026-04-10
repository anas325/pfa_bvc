"""
RSS sentiment pipeline entry point.

Fetches articles from configured RSS feeds, runs LLM sentiment analysis,
and stores results in Neo4j. Resumable — articles already in Neo4j are skipped.

Usage:
    cd Pipelines
    uv run python -m rss.pipeline
"""

import csv
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent
CONFIG_FILE = ROOT / "config" / "rss_feeds.yaml"
COMPANIES_CSV = ROOT / "data" / "companies.csv"

# Import after load_dotenv so env vars are available
from db.db import get_driver
from rss.rss_fetcher import fetch_all_feeds, filter_by_company_mentions
from rss.sentiment import analyze_batch, build_sentiment_llm
from rss.neo4j_loader import (
    ensure_constraints,
    get_already_processed_urls,
    seed_companies,
    store_batch,
)


def load_config() -> dict:
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_companies() -> list[dict]:
    with open(COMPANIES_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> None:
    config = load_config()
    companies = load_companies()
    llm_cfg = config.get("llm", {})
    batch_size = llm_cfg.get("batch_size", 5)
    known_tickers = [c.get("ticker", "").strip() for c in companies if c.get("ticker", "").strip()]

    print("=== BVC RSS Sentiment Pipeline ===\n")

    driver = get_driver()
    driver.verify_connectivity()

    print("Setting up Neo4j schema...")
    ensure_constraints(driver)
    seed_companies(driver, companies)
    print(f"  Seeded {len(companies)} companies.\n")

    print("Fetching RSS feeds...")
    articles = fetch_all_feeds(config)
    print()

    article_ticker_pairs = filter_by_company_mentions(articles, companies)

    done_urls = get_already_processed_urls(driver)
    to_process = [
        (a, t) for a, t in article_ticker_pairs if a.url not in done_urls
    ]
    print(f"Resuming — {len(done_urls)} already analyzed, {len(to_process)} to analyze.\n")

    if not to_process:
        print("Nothing new to analyze. Done.")
        driver.close()
        return

    sentiment_llm = build_sentiment_llm(llm_cfg)

    for i in range(0, len(to_process), batch_size):
        batch = to_process[i : i + batch_size]
        batch_articles = [a for a, _ in batch]
        tickers_map = {a.url: t for a, t in batch}

        results = analyze_batch(sentiment_llm, batch_articles, known_tickers, done_urls)

        store_results = [
            (article, sentiment, tickers_map[article.url])
            for article, sentiment in results
        ]
        store_batch(driver, store_results)
        done_urls.update(article.url for article, _ in results)

        completed = min(i + batch_size, len(to_process))
        print(f"  {completed}/{len(to_process)} articles analyzed and stored.")

    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    missing = [v for v in ("OPENROUTER_API_KEY", "NEO4J_PASSWORD") if not os.getenv(v)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    main()
