"""
backfill/ingest.py — Step 2 of the correlation pipeline.

Reads CDX-harvested HTML archives from data/{domain}/index.parquet,
runs LLM sentiment analysis via rss.analyzer.LLMArticleAnalyzer,
and writes results to PostgreSQL (primary) and optionally Neo4j.

Resumable: articles already in PG are skipped.


cd backfill && uv sync
uv run python ingest.py --domain lematin --dry-run --limit 10
uv run python ingest.py --domain lematin --batch-size 5 --limit 5
"""

import gzip
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import pyarrow.parquet as pq
import typer
from dotenv import load_dotenv
from selectolax.parser import HTMLParser

# ---------------------------------------------------------------------------
# Path helpers — resolve before any relative imports
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent
_PIPELINES = _HERE.parent / "Pipelines"
_PIPELINES_DATA = _PIPELINES / "data"
_SQL_DIR = _PIPELINES / "db" / "sql"

# Ensure Pipelines is importable even if the editable install isn't active.
if str(_PIPELINES) not in sys.path:
    sys.path.insert(0, str(_PIPELINES))

from rss.analyzer import build_analyzer  # noqa: E402
from rss.models import ArticleSentiment  # noqa: E402
from rss.rss_fetcher import Article  # noqa: E402


# ---------------------------------------------------------------------------
# Domain registry
# ---------------------------------------------------------------------------

@dataclass
class DomainSpec:
    name: str
    language: str
    text_selector: str
    feed_name: str = field(default="")

    def __post_init__(self) -> None:
        if not self.feed_name:
            self.feed_name = f"Backfill: {self.name}"

    @property
    def feed_url(self) -> str:
        return f"backfill://{self.name}"


DOMAIN_REGISTRY: dict[str, DomainSpec] = {
    "lematin":       DomainSpec("lematin",       "fr", ".detail-article"),
    "challenge":     DomainSpec("challenge",     "fr", ".entry-content"),
    "lavieeco":      DomainSpec("lavieeco",      "fr", ".article-body"),
    "leconomiste":   DomainSpec("leconomiste",   "fr", ".field-items"),
    "medias24":      DomainSpec("medias24",      "fr", ".article-content"),
    "hespress_fr":   DomainSpec("hespress_fr",   "fr", ".article-body"),
    "finances_news": DomainSpec("finances_news", "fr", ".entry-content"),
    "leseco":        DomainSpec("leseco",        "fr", ".article-content"),
}


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------

def _sql(name: str) -> str:
    return (_SQL_DIR / name).read_text(encoding="utf-8")


def _na(val):
    return None if val is None else val


# ---------------------------------------------------------------------------
# Parquet helpers
# ---------------------------------------------------------------------------

def check_parquet_integrity(path: Path) -> None:
    """Abort with a helpful message if the parquet file is truncated."""
    try:
        with open(path, "rb") as fh:
            fh.seek(-4, 2)
            tail = fh.read(4)
    except OSError as e:
        print(f"[ERROR] Cannot read {path}: {e}")
        raise SystemExit(1)

    if tail != b"PAR1":
        print(f"[ERROR] Parquet file appears truncated: {path}")
        print("  Re-run the harvester for this domain to complete the archive.")
        raise SystemExit(1)


def load_index(domain: str, data_dir: Path) -> list[dict]:
    """Load, filter, and dedup the index.parquet for a domain."""
    index_path = data_dir / domain / "index.parquet"
    check_parquet_integrity(index_path)

    rows = pq.read_table(index_path).to_pylist()

    # Keep only successful captures with a stored HTML path.
    rows = [r for r in rows if r.get("status") == 200 and r.get("raw_html_path")]

    # Dedup by URL: prefer row with original_published_date; break ties by capture_timestamp.
    best: dict[str, dict] = {}
    for row in rows:
        url = row["url"]
        if url not in best:
            best[url] = row
            continue
        prev = best[url]
        prev_has_date = bool(prev.get("original_published_date"))
        curr_has_date = bool(row.get("original_published_date"))
        if curr_has_date and not prev_has_date:
            best[url] = row
        elif curr_has_date == prev_has_date:
            if (row.get("capture_timestamp") or "") > (prev.get("capture_timestamp") or ""):
                best[url] = row

    return list(best.values())


def resolve_published_at(row: dict) -> datetime:
    """Extract a UTC datetime from the index row."""
    dt = row.get("original_published_date")
    if dt is not None:
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        # PyArrow sometimes yields a date — promote to midnight UTC.
        try:
            from datetime import date as date_type
            if isinstance(dt, date_type):
                return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
        except Exception:
            pass

    ts = row.get("capture_timestamp", "")
    try:
        return datetime.strptime(str(ts)[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def read_html(domain_dir: Path, raw_html_path: str) -> str | None:
    """Read a gzip-compressed HTML file; return None on any IO error."""
    path = domain_dir / raw_html_path
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except (OSError, EOFError):
        return None


def extract_text(html: str, selector: str) -> str:
    """Extract visible text using a CSS selector; returns "" if not found."""
    node = HTMLParser(html).css_first(selector)
    if node is None:
        return ""
    return node.text(separator="\n", strip=True)


def extract_title(html: str) -> str:
    """Extract page title from <title> or og:title meta tag."""
    tree = HTMLParser(html)
    title_node = tree.css_first("title")
    if title_node:
        text = title_node.text(strip=True)
        if text:
            return text
    og = tree.css_first('meta[property="og:title"]')
    if og:
        content = og.attributes.get("content", "")
        if content:
            return content.strip()
    return ""


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "pfa_bvc"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def get_pg_known_urls(conn, feed_url: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT url FROM articles WHERE feed_url = %s", (feed_url,))
        return {row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# LLM config
# ---------------------------------------------------------------------------

def build_llm_cfg() -> dict:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("[ERROR] OPENROUTER_API_KEY is not set.")
        raise SystemExit(1)
    return {
        "analyzer": "llm",
        "base_url": os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
        "model": os.getenv("OPENROUTER_MODEL", "openrouter/auto"),
        "request_timeout": int(os.getenv("LLM_REQUEST_TIMEOUT", "60")),
        "max_retries": int(os.getenv("LLM_MAX_RETRIES", "2")),
        "entity_extraction": {
            "enabled": True,
            "include_companies": True,
            "include_sectors": True,
        },
    }


# ---------------------------------------------------------------------------
# Batch write helpers
# ---------------------------------------------------------------------------

def _write_batch(
    conn,
    spec: DomainSpec,
    batch: list[tuple[Article, ArticleSentiment]],
    known_tickers: set[str],
    stats: dict,
) -> None:
    """Write a batch to PG and commit. Retries article-by-article on batch failure."""
    try:
        with conn.cursor() as cur:
            for article, sentiment in batch:
                _write_one(cur, spec, article, sentiment, known_tickers)
        conn.commit()
        stats["written"] += len(batch)
    except Exception as batch_err:
        conn.rollback()
        print(f"  [WARN] Batch write failed ({batch_err}), retrying individually...")
        for article, sentiment in batch:
            try:
                with conn.cursor() as cur:
                    _write_one(cur, spec, article, sentiment, known_tickers)
                conn.commit()
                stats["written"] += 1
            except Exception as row_err:
                conn.rollback()
                print(f"  [SKIP] Write failed for {article.url}: {row_err}")
                stats["pg_errors"] += 1


def _write_one(cur, spec: DomainSpec, article: Article, sentiment: ArticleSentiment, known_tickers: set[str]) -> None:
    now = datetime.now(timezone.utc).isoformat()

    cur.execute(_sql("upsert_article.sql"), (
        article.url, article.title, article.published_at,
        article.full_text, article.language, spec.feed_url,
    ))
    cur.execute(_sql("upsert_sentiment.sql"), (
        article.url, sentiment.sentiment, sentiment.score,
        sentiment.confidence, sentiment.reasoning, now,
    ))
    for ticker in known_tickers & set(sentiment.mentioned_tickers):
        cur.execute(_sql("upsert_company_mention.sql"), (article.url, ticker))
    for sector in sentiment.mentioned_sectors:
        cur.execute(_sql("upsert_sector_mention.sql"), (article.url, sector))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(add_completion=False)


@app.command()
def ingest(
    domain: str = typer.Option(..., help="Domain key from DOMAIN_REGISTRY"),
    batch_size: int = typer.Option(50, help="Articles per PG commit"),
    dry_run: bool = typer.Option(False, help="Extract text only — no LLM or DB writes"),
    data_dir: Path = typer.Option(Path("data"), help="Root directory for domain archives"),
    limit: Optional[int] = typer.Option(None, help="Cap the number of articles to process"),
    neo4j: bool = typer.Option(False, help="Also write articles and sentiment to Neo4j"),
) -> None:
    """Ingest CDX-harvested HTML archives into PostgreSQL with LLM sentiment analysis."""

    # 1. Validate domain
    if domain not in DOMAIN_REGISTRY:
        print(f"[ERROR] Unknown domain '{domain}'. Available: {', '.join(DOMAIN_REGISTRY)}")
        raise typer.Exit(1)
    spec = DOMAIN_REGISTRY[domain]

    # 2. Load environment
    env_path = _HERE / ".env"
    if not env_path.exists():
        env_path = _HERE.parent / "Pipelines" / ".env"
    load_dotenv(env_path)

    # 3. Load + filter index
    print(f"Loading index for '{domain}'...")
    rows = load_index(domain, data_dir)
    print(f"  {len(rows)} rows after filter+dedup")

    # 4. Open PG, load known URLs, filter
    print("Connecting to PostgreSQL...")
    conn = get_pg_connection()
    known_urls = get_pg_known_urls(conn, spec.feed_url)
    new_rows = [r for r in rows if r["url"] not in known_urls]
    print(f"  {len(rows)} total rows, {len(known_urls)} already processed, {len(new_rows)} to ingest")

    if not new_rows:
        print("Nothing to do.")
        conn.close()
        return

    if limit is not None:
        new_rows = new_rows[:limit]
        print(f"  Limited to {len(new_rows)} articles")

    # 5. Load companies + build analyzer
    companies_df = pd.read_csv(_PIPELINES_DATA / "companies.csv", dtype=str)
    companies = companies_df.to_dict("records")
    known_tickers = {c["ticker"].strip() for c in companies if c.get("ticker")}

    if not dry_run:
        llm_cfg = build_llm_cfg()
        analyzer = build_analyzer(llm_cfg, companies, sectors=[])

    # 6. Optional Neo4j setup
    neo4j_driver = None
    if neo4j and not dry_run:
        from neo4j import GraphDatabase
        from rss.neo4j_loader import ensure_constraints, store_articles_raw, store_sentiment_batch
        neo4j_driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=("neo4j", os.getenv("NEO4J_PASSWORD")),
        )
        ensure_constraints(neo4j_driver)
        print("  Neo4j connected and constraints ensured")

    # 7. Upsert synthetic feed row
    if not dry_run:
        with conn.cursor() as cur:
            cur.execute(_sql("upsert_feed.sql"), (spec.feed_url, spec.feed_name, spec.language))
        conn.commit()

    # 8. Process in batches
    domain_dir = data_dir / domain
    stats: dict = {
        "written": 0,
        "skipped_no_html": 0,
        "skipped_no_text": 0,
        "llm_errors": 0,
        "pg_errors": 0,
    }

    batch: list[tuple[Article, ArticleSentiment]] = []
    total = len(new_rows)

    for i, row in enumerate(new_rows, start=1):
        url = row["url"]

        # Read HTML
        html = read_html(domain_dir, row["raw_html_path"])
        if html is None:
            stats["skipped_no_html"] += 1
            continue

        text = extract_text(html, spec.text_selector)
        if not text:
            stats["skipped_no_text"] += 1
            continue

        title = extract_title(html) or url
        published_at = resolve_published_at(row)

        article = Article(
            url=url,
            feed_name=spec.feed_name,
            feed_url=spec.feed_url,
            title=title,
            published_at=published_at,
            full_text=text,
            language=spec.language,
        )

        if dry_run:
            print(f"  [{i}/{total}] DRY-RUN: {title[:80]}")
            continue

        # LLM analysis
        try:
            sentiment = analyzer.analyze(article)
        except Exception as e:
            print(f"  [SKIP] LLM error for {url}: {e}")
            stats["llm_errors"] += 1
            continue

        print(f"  [{i}/{total}] {sentiment.sentiment:8s} | {title[:60]}")

        # Optional Neo4j write
        if neo4j_driver is not None:
            try:
                store_articles_raw(neo4j_driver, [article])
                store_sentiment_batch(neo4j_driver, [(url, sentiment)])
            except Exception as e:
                print(f"  [WARN] Neo4j write failed for {url}: {e}")

        batch.append((article, sentiment))

        if len(batch) >= batch_size:
            _write_batch(conn, spec, batch, known_tickers, stats)
            batch.clear()

    # Flush remaining
    if batch:
        _write_batch(conn, spec, batch, known_tickers, stats)

    conn.close()
    if neo4j_driver is not None:
        neo4j_driver.close()

    # 9. Summary
    print("\n--- Summary ---")
    print(f"  Written:          {stats['written']}")
    print(f"  Skipped (no HTML):{stats['skipped_no_html']}")
    print(f"  Skipped (no text):{stats['skipped_no_text']}")
    print(f"  LLM errors:       {stats['llm_errors']}")
    print(f"  PG errors:        {stats['pg_errors']}")


@app.command("debug-selectors")
def debug_selectors(
    domain: str = typer.Option(..., help="Domain key from DOMAIN_REGISTRY"),
    data_dir: Path = typer.Option(Path("data")),
    sample: int = typer.Option(5, help="Number of HTML files to inspect"),
    candidates: str = typer.Option(
        "",
        help="Comma-separated extra CSS selectors to try alongside the registered one",
    ),
) -> None:
    """Inspect sample HTML files and report which CSS selectors yield text."""
    if domain not in DOMAIN_REGISTRY:
        print(f"[ERROR] Unknown domain '{domain}'. Available: {', '.join(DOMAIN_REGISTRY)}")
        raise typer.Exit(1)
    spec = DOMAIN_REGISTRY[domain]

    extra = [s.strip() for s in candidates.split(",") if s.strip()]
    selectors = [spec.text_selector] + extra

    rows = load_index(domain, data_dir)
    domain_dir = data_dir / domain
    print(f"Sampling {min(sample, len(rows))} of {len(rows)} rows for domain '{domain}'\n")

    inspected = 0
    for row in rows:
        if inspected >= sample:
            break
        html = read_html(domain_dir, row["raw_html_path"])
        if html is None:
            print(f"  [MISSING] {row['raw_html_path']}")
            continue

        inspected += 1
        print(f"  URL: {row['url']}")
        print(f"  File: {row['raw_html_path']}  ({len(html):,} chars)")
        print(f"  Title tag: {extract_title(html)[:80]!r}")

        # Report hit/miss for each candidate selector
        for sel in selectors:
            text = extract_text(html, sel)
            tag = "[registered]" if sel == spec.text_selector else "[candidate] "
            if text:
                preview = text[:120].replace("\n", " ")
                print(f"  {tag} {sel!r}  => HIT  ({len(text)} chars) — {preview!r}")
            else:
                print(f"  {tag} {sel!r}  => miss")

        # Show all class names present in the page to help spot the right selector
        tree = HTMLParser(html)
        classes: set[str] = set()
        for node in tree.css("[class]"):
            for cls in (node.attributes.get("class") or "").split():
                classes.add(cls)
        article_classes = sorted(c for c in classes if any(
            kw in c.lower() for kw in ("article", "content", "body", "text", "entry", "desc", "post")
        ))
        if article_classes:
            print(f"  Candidate classes: {article_classes}")
        print()


if __name__ == "__main__":
    app()
