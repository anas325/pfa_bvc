"""
RSS feed fetcher.

Fetches and parses RSS/Atom feeds using feedparser, filters by recency,
and optionally matches articles to BVC company names/tickers.
"""

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent
CONFIG_FILE = ROOT / "config" / "rss_feeds.yaml"

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    """Remove HTML tags and unescape HTML entities."""
    import html
    return html.unescape(_HTML_TAG_RE.sub("", text or "")).strip()


def _parse_date(entry: dict) -> datetime:
    """Extract a timezone-aware datetime from a feedparser entry."""
    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                continue
    return datetime.now(timezone.utc)


@dataclass
class Article:
    url: str
    feed_name: str
    feed_url: str
    title: str
    published_at: datetime
    full_text: str
    language: str


# ---------------------------------------------------------------------------
# Full-article scrapers
# ---------------------------------------------------------------------------
# Each scraper receives (url: str, timeout: int) and returns the article text.
# Register new scrapers here; the key matches the `scraper:` field in rss_feeds.yaml.

def _scrape_lematin(url: str, timeout: int) -> str:
    """Fetch a LeMatin article and extract the body from .article-desc."""
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": feedparser.USER_AGENT})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    tag = soup.find(class_="article-desc")
    return tag.get_text(separator="\n", strip=True) if tag else ""


SCRAPERS: dict[str, Callable[[str, int], str]] = {
    "lematin": _scrape_lematin,
}


def scrape_full_text(url: str, scraper_name: str, timeout: int) -> str:
    """
    Dispatch to the named scraper.  Returns empty string on any failure so
    the caller can fall back to the RSS summary.
    """
    scraper = SCRAPERS.get(scraper_name)
    if scraper is None:
        raise KeyError(f"No scraper registered for '{scraper_name}'")
    return scraper(url, timeout)


# ---------------------------------------------------------------------------

def load_feeds_config(config_path: Path = CONFIG_FILE) -> dict:
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_feed(feed: dict, lookback_days: int, timeout: int) -> list[Article]:
    """Fetch a single RSS/Atom feed, returning articles within the lookback window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    try:
        parsed = feedparser.parse(feed["url"], request_headers={"Connection": "close"})
    except Exception as e:
        print(f"  [WARN] Could not fetch feed '{feed['name']}': {e}")
        return []

    if parsed.get("bozo") and not parsed.get("entries"):
        print(f"  [WARN] Malformed feed '{feed['name']}': {parsed.get('bozo_exception', 'unknown error')}")
        return []

    articles = []
    for entry in parsed.get("entries", []):
        url = entry.get("link") or entry.get("id", "")
        title = _strip_html(entry.get("title", ""))
        if not url or not title:
            continue

        published_at = _parse_date(entry)
        if published_at < cutoff:
            continue

        scraper_name = feed.get("scraper")
        if scraper_name:
            try:
                full_text = scrape_full_text(url, scraper_name, timeout) or title
            except Exception as e:
                print(f"    [WARN] Scraper '{scraper_name}' failed for {url}: {e}")
                summary = entry.get("summary") or entry.get("content", [{}])[0].get("value", "")
                full_text = _strip_html(summary) or title
        else:
            summary = entry.get("summary") or entry.get("content", [{}])[0].get("value", "")
            full_text = _strip_html(summary) or title

        articles.append(Article(
            url=url,
            feed_name=feed["name"],
            feed_url=feed["url"],
            title=title,
            published_at=published_at,
            full_text=full_text,
            language=feed.get("language", "fr"),
        ))

    return articles


def fetch_all_feeds(config: dict) -> list[Article]:
    """
    Fetch all enabled feeds from config, deduplicating articles by URL.
    Returns articles sorted newest-first.
    """
    fetcher_cfg = config.get("fetcher", {})
    lookback_days = fetcher_cfg.get("lookback_days", 7)
    timeout = fetcher_cfg.get("request_timeout", 15)
    feedparser.USER_AGENT = fetcher_cfg.get("user_agent", "BVC-Research-Bot/1.0")

    seen_urls: set[str] = set()
    all_articles: list[Article] = []

    for feed in config.get("feeds", []):
        if not feed.get("enabled", True):
            continue
        print(f"  Fetching: {feed['name']} ...")
        articles = fetch_feed(feed, lookback_days, timeout)
        for article in articles:
            if article.url not in seen_urls:
                seen_urls.add(article.url)
                all_articles.append(article)
        print(f"    {len(articles)} articles (before dedup)")

    all_articles.sort(key=lambda a: a.published_at, reverse=True)
    print(f"  Total unique articles: {len(all_articles)}")
    return all_articles


def _normalize(text: str) -> str:
    """Lowercase and strip accents for language-agnostic matching."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _wb(term: str) -> re.Pattern:
    """Compile a case-insensitive word-boundary pattern for term."""
    return re.compile(r"\b" + re.escape(term) + r"\b", re.IGNORECASE)


def match_article_entities(
    articles: list[Article],
    companies: list[dict],
    require_match: bool = False,
) -> list[tuple[Article, list[str], list[str]]]:
    """
    Match articles against company tickers, names, and sector names using
    word-boundary regex on normalized text.

    Returns list of (Article, matched_tickers, matched_sectors).
    If require_match=True, drops articles with no company or sector matches.
    """
    # --- company patterns ---
    ticker_patterns: list[tuple[re.Pattern, str]] = []
    name_patterns: list[tuple[re.Pattern, str]] = []
    for c in companies:
        ticker = c.get("ticker", "").strip()
        name = c.get("libelle", "").strip()
        if ticker:
            ticker_patterns.append((_wb(ticker), ticker))
        if name and len(name) > 3:
            name_patterns.append((_wb(_normalize(name)), ticker))

    # --- sector patterns (unique, strip "MASI " prefix) ---
    seen: set[str] = set()
    sector_patterns: list[tuple[re.Pattern, str]] = []
    for c in companies:
        raw = c.get("secteur", "").strip()
        sector = re.sub(r"^MASI\s+", "", raw).strip()
        if sector and sector not in seen and len(sector) > 3:
            seen.add(sector)
            sector_patterns.append((_wb(_normalize(sector)), sector))

    results = []
    for article in articles:
        haystack_raw = article.title + " " + article.full_text
        haystack_norm = _normalize(haystack_raw)

        matched_tickers = list({
            ticker
            for pattern, ticker in ticker_patterns
            if pattern.search(haystack_raw)
        } | {
            ticker
            for pattern, ticker in name_patterns
            if pattern.search(haystack_norm)
        })

        matched_sectors = list({
            sector
            for pattern, sector in sector_patterns
            if pattern.search(haystack_norm)
        })

        if require_match and not matched_tickers and not matched_sectors:
            continue

        results.append((article, matched_tickers, matched_sectors))

    return results
