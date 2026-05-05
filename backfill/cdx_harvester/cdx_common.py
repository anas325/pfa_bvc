"""Core CDX harvester: query Wayback CDX, download captures, parse dates, persist parquet."""
from __future__ import annotations

import gzip
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from selectolax.parser import HTMLParser
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .log import setup_logger

CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WAYBACK_RAW = "https://web.archive.org/web/{ts}id_/{url}"

# Schema for index parquet — one row per (original_url, capture_timestamp).
INDEX_SCHEMA = pa.schema([
    ("url", pa.string()),
    ("capture_timestamp", pa.string()),         # CDX format: YYYYMMDDhhmmss
    ("original_published_date", pa.timestamp("s", tz="UTC")),
    ("raw_html_path", pa.string()),
    ("digest", pa.string()),
    ("status", pa.int32()),
    ("mimetype", pa.string()),
    ("length", pa.int64()),
])


@dataclass
class DomainConfig:
    """Per-domain harvest configuration. Each domain script declares one of these."""
    name: str                              # short id, e.g. "lematin"
    cdx_url_pattern: str                   # e.g. "lematin.ma/*"
    date_from: str = "20210101"            # CDX 'from' (YYYYMMDD or longer)
    date_to: str = ""                      # CDX 'to' — empty means "now"
    include_regex: str | None = None       # if set, original URL must match
    exclude_regex: str | None = None       # if set, original URL must NOT match
    collapse: str = "urlkey"               # "urlkey" | "digest" | ""
    cdx_page_size: int = 10_000            # rows per CDX page
    cdx_rate_per_sec: float = 1.0          # CDX query rate cap
    fetch_concurrency: int = 4             # concurrent raw HTML downloads
    fetch_rate_per_sec: float = 4.0        # raw HTML download rate cap
    extra_filters: list[str] = field(default_factory=list)  # raw CDX filter strings

    def data_dir(self, root: Path) -> Path:
        return root / self.name


# ---------------------------------------------------------------------------
# CDX index phase
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Simple token-bucket-ish limiter: ensures min interval between calls."""
    def __init__(self, per_sec: float):
        self.min_interval = 1.0 / max(per_sec, 0.01)
        self._last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        delta = now - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.monotonic()


@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(6),
    reraise=True,
)
def _cdx_request(client: httpx.Client, params: list[tuple[str, str]]) -> str:
    r = client.get(CDX_ENDPOINT, params=params, timeout=120)
    r.raise_for_status()
    return r.text


def cdx_iter(cfg: DomainConfig, logger) -> Iterator[dict]:
    """Stream CDX rows for a domain, paginating via resumeKey.

    Yields dicts with keys: url, capture_timestamp, digest, status, mimetype, length.
    """
    limiter = _RateLimiter(cfg.cdx_rate_per_sec)
    headers = {"User-Agent": "cdx-harvester/0.1 (+research)"}
    resume_key: str | None = None
    page_n = 0

    base_filters = ["statuscode:200", "mimetype:text/html"] + list(cfg.extra_filters)

    with httpx.Client(headers=headers, follow_redirects=True) as client:
        while True:
            params: list[tuple[str, str]] = [
                ("url", cfg.cdx_url_pattern),
                ("output", "json"),
                ("from", cfg.date_from),
                ("limit", str(cfg.cdx_page_size)),
                ("showResumeKey", "true"),
                ("fl", "original,timestamp,digest,statuscode,mimetype,length"),
            ]
            if cfg.date_to:
                params.append(("to", cfg.date_to))
            if cfg.collapse:
                params.append(("collapse", cfg.collapse))
            for f in base_filters:
                params.append(("filter", f))
            if resume_key:
                params.append(("resumeKey", resume_key))

            limiter.wait()
            text = _cdx_request(client, params)
            stripped = text.strip()
            if not stripped:
                logger.info("CDX page %d: empty — done", page_n)
                return

            # CDX output=json returns a single JSON array of arrays:
            # [[header...], [row...], [row...], ..., [resumeKey]?]
            try:
                rows = json.loads(stripped)
            except json.JSONDecodeError:
                # Fall back to legacy NDJSON-style (one array per line) just in case.
                try:
                    rows = [json.loads(ln) for ln in stripped.splitlines() if ln.strip()]
                except json.JSONDecodeError:
                    logger.warning(
                        "CDX page %d: non-JSON response (first 500 chars): %r",
                        page_n, text[:500],
                    )
                    return

            if not rows:
                logger.info("CDX page %d: empty — done", page_n)
                return

            header = rows[0]
            data_rows = rows[1:]
            new_resume_key: str | None = None

            # showResumeKey=true appends the key as a trailing 1-element row.
            if data_rows and isinstance(data_rows[-1], list) and len(data_rows[-1]) == 1:
                new_resume_key = data_rows[-1][0]
                data_rows = data_rows[:-1]

            for row in data_rows:
                if len(row) != len(header):
                    continue
                rec = dict(zip(header, row))
                try:
                    yield {
                        "url": rec.get("original", ""),
                        "capture_timestamp": rec.get("timestamp", ""),
                        "digest": rec.get("digest", ""),
                        "status": int(rec.get("statuscode") or 0),
                        "mimetype": rec.get("mimetype", ""),
                        "length": int(rec.get("length") or 0),
                    }
                except (ValueError, TypeError):
                    continue

            logger.info("CDX page %d: %d rows (resume=%s)", page_n, len(data_rows), bool(new_resume_key))
            page_n += 1

            if not new_resume_key:
                return
            resume_key = new_resume_key


def _filter_row(row: dict, cfg: DomainConfig, inc: re.Pattern | None, exc: re.Pattern | None) -> bool:
    url = row["url"]
    if inc and not inc.search(url):
        return False
    if exc and exc.search(url):
        return False
    return True


def write_index(cfg: DomainConfig, root: Path, logger) -> Path:
    """Phase 1: pull CDX index for the domain and write index.parquet."""
    out_dir = cfg.data_dir(root)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "index.parquet"

    inc = re.compile(cfg.include_regex) if cfg.include_regex else None
    exc = re.compile(cfg.exclude_regex) if cfg.exclude_regex else None

    # Buffered batch write — 50k rows per row-group is plenty.
    batch_size = 50_000
    buf: list[dict] = []
    seen: set[tuple[str, str]] = set()  # (url, capture_timestamp) dedup safety
    writer: pq.ParquetWriter | None = None
    total_kept = 0
    total_seen = 0

    def flush():
        nonlocal writer, buf
        if not buf:
            return
        cols = {
            "url": [r["url"] for r in buf],
            "capture_timestamp": [r["capture_timestamp"] for r in buf],
            "original_published_date": [None] * len(buf),
            "raw_html_path": [None] * len(buf),
            "digest": [r["digest"] for r in buf],
            "status": [r["status"] for r in buf],
            "mimetype": [r["mimetype"] for r in buf],
            "length": [r["length"] for r in buf],
        }
        table = pa.table(cols, schema=INDEX_SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(out_path, INDEX_SCHEMA, compression="zstd")
        writer.write_table(table)
        buf.clear()

    try:
        for row in cdx_iter(cfg, logger):
            total_seen += 1
            if not _filter_row(row, cfg, inc, exc):
                continue
            key = (row["url"], row["capture_timestamp"])
            if key in seen:
                continue
            seen.add(key)
            buf.append(row)
            total_kept += 1
            if len(buf) >= batch_size:
                flush()
                logger.info("indexed %d (kept) / %d (seen)", total_kept, total_seen)
        flush()
    finally:
        if writer is not None:
            writer.close()

    logger.info("phase=index done: kept=%d seen=%d -> %s", total_kept, total_seen, out_path)
    return out_path


# ---------------------------------------------------------------------------
# Fetch phase
# ---------------------------------------------------------------------------

# Cheap, fast metadata-date extraction. Order matters: most reliable first.
_DATE_META_SELECTORS = [
    'meta[property="article:published_time"]',
    'meta[name="article:published_time"]',
    'meta[property="og:published_time"]',
    'meta[name="og:published_time"]',
    'meta[name="pubdate"]',
    'meta[name="publish-date"]',
    'meta[name="date"]',
    'meta[itemprop="datePublished"]',
]


def _try_parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    # Common variations
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    for fmt in (None, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.fromisoformat(s) if fmt is None else datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


def extract_published_date(html: str) -> datetime | None:
    """Best-effort article date extraction from HTML metadata."""
    if not html:
        return None
    try:
        tree = HTMLParser(html)
    except Exception:
        return None

    for sel in _DATE_META_SELECTORS:
        for node in tree.css(sel):
            content = node.attributes.get("content")
            if content:
                dt = _try_parse_iso(content)
                if dt:
                    return dt

    # JSON-LD fallback
    for node in tree.css('script[type="application/ld+json"]'):
        text = node.text() or ""
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            continue
        candidates = data if isinstance(data, list) else [data]
        for d in candidates:
            if isinstance(d, dict):
                v = d.get("datePublished") or d.get("dateCreated")
                if isinstance(v, str):
                    dt = _try_parse_iso(v)
                    if dt:
                        return dt

    # <time datetime="..."> fallback
    for node in tree.css("time[datetime]"):
        v = node.attributes.get("datetime")
        if v:
            dt = _try_parse_iso(v)
            if dt:
                return dt

    return None


def _raw_path_for(out_dir: Path, ts: str, digest: str) -> Path:
    yyyy = ts[:4] if len(ts) >= 4 else "unknown"
    mm = ts[4:6] if len(ts) >= 6 else "00"
    return out_dir / "raw" / yyyy / mm / f"{digest}.html.gz"


@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _fetch_capture(client: httpx.Client, url: str, ts: str) -> bytes:
    target = WAYBACK_RAW.format(ts=ts, url=url)
    r = client.get(target, timeout=60)
    r.raise_for_status()
    return r.content


def fetch_pending(cfg: DomainConfig, root: Path, logger, max_articles: int | None = None) -> int:
    """Phase 2: download HTML for rows missing raw_html_path, parse published_date, rewrite parquet."""
    out_dir = cfg.data_dir(root)
    index_path = out_dir / "index.parquet"
    if not index_path.exists():
        logger.error("no index.parquet at %s — run phase=index first", index_path)
        return 0

    table = pq.read_table(index_path)
    df = table.to_pylist()

    pending_idx = [i for i, row in enumerate(df) if not row.get("raw_html_path")]
    if max_articles is not None:
        pending_idx = pending_idx[:max_articles]
    logger.info("phase=fetch: %d pending of %d total", len(pending_idx), len(df))

    limiter = _RateLimiter(cfg.fetch_rate_per_sec)
    headers = {"User-Agent": "cdx-harvester/0.1 (+research)"}
    fetched = 0
    flush_every = 500

    def save_progress():
        new_table = pa.Table.from_pylist(df, schema=INDEX_SCHEMA)
        tmp = index_path.with_suffix(".parquet.tmp")
        pq.write_table(new_table, tmp, compression="zstd")
        tmp.replace(index_path)

    with httpx.Client(headers=headers, follow_redirects=True) as client:
        for n, i in enumerate(pending_idx, 1):
            row = df[i]
            ts = row["capture_timestamp"]
            url = row["url"]
            digest = row["digest"] or "nodigest"
            raw_path = _raw_path_for(out_dir, ts, digest)

            if raw_path.exists():
                # File already on disk (e.g. interrupted prior run) — link it back, parse date.
                row["raw_html_path"] = str(raw_path.relative_to(out_dir))
                try:
                    with gzip.open(raw_path, "rt", encoding="utf-8", errors="replace") as f:
                        html = f.read()
                    pub = extract_published_date(html)
                    if pub:
                        row["original_published_date"] = pub
                except OSError:
                    pass
                fetched += 1
            else:
                limiter.wait()
                try:
                    content = _fetch_capture(client, url, ts)
                except httpx.HTTPError as e:
                    logger.warning("fetch failed [%s] %s %s -> %s", ts, url, type(e).__name__, e)
                    continue

                raw_path.parent.mkdir(parents=True, exist_ok=True)
                with gzip.open(raw_path, "wb", compresslevel=6) as f:
                    f.write(content)

                row["raw_html_path"] = str(raw_path.relative_to(out_dir))
                try:
                    html = content.decode("utf-8", errors="replace")
                    pub = extract_published_date(html)
                    if pub:
                        row["original_published_date"] = pub
                except Exception:
                    pass
                fetched += 1

            if n % 50 == 0:
                logger.info("fetched %d / %d", n, len(pending_idx))
            if n % flush_every == 0:
                save_progress()
                logger.info("checkpoint written at %d", n)

    save_progress()
    logger.info("phase=fetch done: %d articles fetched in this run", fetched)
    return fetched


# ---------------------------------------------------------------------------
# Stats helpers (used by dashboard)
# ---------------------------------------------------------------------------

def domain_stats(cfg: DomainConfig, root: Path) -> dict:
    out_dir = cfg.data_dir(root)
    index_path = out_dir / "index.parquet"
    if not index_path.exists():
        return {
            "name": cfg.name, "indexed": 0, "fetched": 0, "pending": 0,
            "oldest": None, "newest": None, "by_month": {},
        }
    table = pq.read_table(index_path, columns=["capture_timestamp", "raw_html_path", "original_published_date"])
    n = table.num_rows
    raw_paths = table.column("raw_html_path").to_pylist()
    fetched = sum(1 for p in raw_paths if p)
    timestamps = table.column("capture_timestamp").to_pylist()
    pubs = table.column("original_published_date").to_pylist()

    by_month: dict[str, int] = {}
    for p, t in zip(pubs, timestamps):
        # Prefer parsed publication date, fall back to capture timestamp.
        if p is not None:
            key = p.strftime("%Y-%m") if hasattr(p, "strftime") else str(p)[:7]
        elif t and len(t) >= 6:
            key = f"{t[:4]}-{t[4:6]}"
        else:
            continue
        by_month[key] = by_month.get(key, 0) + 1

    nonempty_ts = [t for t in timestamps if t]
    return {
        "name": cfg.name,
        "indexed": n,
        "fetched": fetched,
        "pending": n - fetched,
        "oldest": min(nonempty_ts) if nonempty_ts else None,
        "newest": max(nonempty_ts) if nonempty_ts else None,
        "by_month": dict(sorted(by_month.items())),
    }
