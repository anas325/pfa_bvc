"""
RSS feed fetcher.

Fetches and parses RSS/Atom feeds using feedparser, filters by recency,
and optionally matches articles to BVC company names/tickers.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import yaml

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


def load_feeds_config(config_path: Path = CONFIG_FILE) -> dict:
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_feed(feed: dict, lookback_days: int, timeout: int) -> list[Article]:
    """Fetch a single RSS/Atom feed, returning articles within the lookback window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    feedparser.USER_AGENT = feed.get("user_agent", "BVC-Research-Bot/1.0")

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


def filter_by_company_mentions(
    articles: list[Article],
    companies: list[dict],
    require_match: bool = False,
) -> list[tuple[Article, list[str]]]:
    """
    For each article, scan title+full_text for company names and tickers.
    Returns list of (Article, matched_tickers).
    If require_match=True, drops articles with no matches.
    """
    # Build lookup: normalized name/ticker -> canonical ticker
    lookup: list[tuple[str, str]] = []
    for c in companies:
        ticker = c.get("ticker", "").strip()
        name = c.get("company_name", "").strip()
        if ticker:
            lookup.append((ticker.lower(), ticker))
        if name and len(name) > 3:  # avoid matching very short names
            lookup.append((name.lower(), ticker))

    results = []
    for article in articles:
        haystack = (article.title + " " + article.full_text).lower()
        matched = list({
            canonical_ticker
            for term, canonical_ticker in lookup
            if term in haystack
        })
        if require_match and not matched:
            continue
        results.append((article, matched))

    return results
