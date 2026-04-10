"""
RSS sentiment pipeline entry point.

Fetches articles from configured RSS feeds, runs LLM analysis (sentiment +
entity extraction), and stores results in Neo4j. Resumable — articles already
in Neo4j are skipped.

Usage:
    cd Pipelines
    uv run python -m rss.pipeline
"""

import csv
import os
import re
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent
CONFIG_FILE = ROOT / "config" / "rss_feeds.yaml"
COMPANIES_CSV = ROOT / "data" / "companies.csv"

from db.db import get_driver
from rss.analyzer import build_analyzer
from rss.rss_fetcher import fetch_all_feeds
from rss.neo4j_loader import (
    ensure_constraints,
    get_already_processed_urls,
    seed_companies,
    seed_sectors,
    store_batch,
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

    print("=== BVC RSS Sentiment Pipeline ===\n")

    driver = get_driver()
    driver.verify_connectivity()

    print("Setting up Neo4j schema...")
    ensure_constraints(driver)
    seed_companies(driver, companies)
    seed_sectors(driver, companies)
    print(f"  Seeded {len(companies)} companies, {len(sectors)} sectors.\n")

    print("Fetching RSS feeds...")
    articles = fetch_all_feeds(config)
    print()

    done_urls = get_already_processed_urls(driver)
    to_process = [a for a in articles if a.url not in done_urls]
    print(f"Resuming — {len(done_urls)} already analyzed, {len(to_process)} to analyze.\n")

    if not to_process:
        print("Nothing new to analyze. Done.")
        driver.close()
        return

    analyzer = build_analyzer(llm_cfg, companies, sectors)

    for i in range(0, len(to_process), batch_size):
        batch = to_process[i : i + batch_size]
        results = []

        for article in batch:
            try:
                sentiment = analyzer.analyze(article)
                results.append((article, sentiment))
                done_urls.add(article.url)
            except Exception as e:
                print(f"  [WARN] Analysis failed for '{article.title[:60]}': {e}")

        store_batch(driver, results)
        completed = min(i + batch_size, len(to_process))
        print(f"  {completed}/{len(to_process)} articles analyzed and stored.")

    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    missing = [v for v in ("OPENROUTER_API_KEY", "NEO4J_PASSWORD") if not os.getenv(v)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    main()
