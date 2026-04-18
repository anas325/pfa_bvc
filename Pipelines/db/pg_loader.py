"""
Load all BVC data into PostgreSQL.

Sources:
  - Data/companies.csv + companies_detail.csv + companies_research.json + masi_banques.csv -> companies
  - Data/stock_history_clean/*.csv -> stock_prices
  - Neo4j: Feed, Article, SentimentScore, MENTIONS -> feeds, articles, sentiment_scores,
           article_company_mentions, article_sector_mentions
"""

import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "Data"
PIPELINES_DATA = ROOT / "Pipelines" / "data"
SQL_DIR = Path(__file__).parent / "sql"

sys.path.insert(0, str(Path(__file__).parent))
from db import get_driver, get_pg_connection  # noqa: E402


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


TICKER_FROM_FILENAME = re.compile(r"^([A-Z0-9]+) - ")


def _ticker_from_path(p: Path) -> str | None:
    m = TICKER_FROM_FILENAME.match(p.stem)
    return m.group(1) if m else None


def _parse_vol(v) -> float | None:
    if pd.isna(v) or v == "":
        return None
    s = str(v).replace(",", "").strip()
    if s.endswith("K"):
        return float(s[:-1]) * 1_000
    if s.endswith("M"):
        return float(s[:-1]) * 1_000_000
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_companies() -> pd.DataFrame:
    base = pd.read_csv(DATA / "companies.csv", dtype=str)
    base.columns = base.columns.str.strip()

    detail = pd.read_csv(DATA / "companies_detail.csv", dtype=str)
    detail.columns = detail.columns.str.strip()
    detail = detail.rename(columns={"company": "detail_name"})

    masi = pd.read_csv(PIPELINES_DATA / "masi_banques.csv", dtype=str)
    masi.columns = masi.columns.str.strip()

    with open(DATA / "companies_research.json", encoding="utf-8") as f:
        research = pd.DataFrame(json.load(f)).rename(columns={"company_name": "research_name"})

    df = base.copy()

    # detail: match by name substring
    detail_map: dict = {}
    for _, drow in detail.iterrows():
        for _, crow in base.iterrows():
            if drow["detail_name"].lower() in crow["company_name"].lower() or \
               crow["company_name"].lower() in drow["detail_name"].lower():
                detail_map[crow["ticker"]] = drow
                break
    df["sector"] = df["ticker"].map(lambda t: detail_map.get(t, {}).get("sector"))
    df["parent"] = df["ticker"].map(lambda t: detail_map.get(t, {}).get("parent"))

    # masi
    masi_map = masi.set_index("ticker").to_dict("index")
    df["siege_social"] = df["ticker"].map(lambda t: masi_map.get(t, {}).get("siege_social"))
    df["sector"] = df.apply(
        lambda r: r["sector"] or re.sub(
            r"^MASI\s+", "", masi_map.get(r["ticker"], {}).get("secteur", "") or ""
        ).strip() or None,
        axis=1,
    )

    # research
    research_map = research.set_index("ticker").to_dict("index")
    for col in ("description", "ceo", "founded", "headquarters", "revenue", "employees", "stock_exchange"):
        df[col] = df["ticker"].map(lambda t, c=col: research_map.get(t, {}).get(c))

    df["founded"] = pd.to_numeric(df["founded"], errors="coerce").astype("Int64")
    df["employees"] = pd.to_numeric(df["employees"], errors="coerce").astype("Int64")

    return df


def load_stock_prices() -> pd.DataFrame:
    frames = []
    for path in sorted((DATA / "stock_history_clean").glob("*.csv")):
        ticker = _ticker_from_path(path)
        if not ticker:
            continue
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()
        df = df.rename(columns={
            "Date": "date", "Dernier": "close", "Ouv.": "open",
            "Plus Haut": "high", "Plus Bas": "low", "Vol.": "volume", "Variation %": "change_pct",
        })
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["volume"] = df["volume"].apply(_parse_vol)
        for col in ("close", "open", "high", "low", "change_pct"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        frames.append(df.dropna(subset=["date"])[
            ["ticker", "date", "close", "open", "high", "low", "volume", "change_pct"]
        ])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_neo4j() -> dict:
    with get_driver() as driver:
        feeds = [dict(r) for r in driver.execute_query(
            "MATCH (f:Feed) RETURN f.url AS url, f.name AS name, f.language AS language",
            database_="neo4j",
        ).records]

        articles = [dict(r) for r in driver.execute_query(
            "MATCH (a:Article) RETURN a.url AS url, a.title AS title, "
            "a.published_at AS published_at, a.full_text AS full_text, "
            "a.language AS language, a.feed_url AS feed_url",
            database_="neo4j",
        ).records]

        sentiments = [dict(r) for r in driver.execute_query(
            "MATCH (s:SentimentScore) RETURN s.article_url AS article_url, "
            "s.sentiment AS sentiment, s.score AS score, s.confidence AS confidence, "
            "s.reasoning AS reasoning, s.analyzed_at AS analyzed_at",
            database_="neo4j",
        ).records]

        company_mentions = [dict(r) for r in driver.execute_query(
            "MATCH (a:Article)-[:MENTIONS]->(c:Company) RETURN a.url AS article_url, c.ticker AS ticker",
            database_="neo4j",
        ).records]

        sector_mentions = [dict(r) for r in driver.execute_query(
            "MATCH (a:Article)-[:MENTIONS]->(s:Sector) RETURN a.url AS article_url, s.name AS sector_name",
            database_="neo4j",
        ).records]

    return {
        "feeds": feeds,
        "articles": articles,
        "sentiments": sentiments,
        "company_mentions": company_mentions,
        "sector_mentions": sector_mentions,
    }


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def _na(val):
    """Convert pandas NA/NaN to None for psycopg2."""
    return None if pd.isna(val) else val


def upsert_companies(cur, df: pd.DataFrame) -> None:
    q = _sql("upsert_company.sql")
    for _, row in df.iterrows():
        cur.execute(q, (
            row["ticker"], row.get("company_name"),
            _na(row.get("sector")), _na(row.get("parent")),
            _na(row.get("description")), _na(row.get("ceo")),
            _na(row.get("founded")), _na(row.get("headquarters")),
            _na(row.get("revenue")), _na(row.get("employees")),
            _na(row.get("stock_exchange")), _na(row.get("siege_social")),
        ))


def upsert_stock_prices(cur, df: pd.DataFrame) -> None:
    q = _sql("upsert_stock_price.sql")
    for _, row in df.iterrows():
        cur.execute(q, (
            row["ticker"], row["date"],
            _na(row.get("close")), _na(row.get("open")),
            _na(row.get("high")), _na(row.get("low")),
            _na(row.get("volume")), _na(row.get("change_pct")),
        ))


def upsert_neo4j_data(cur, data: dict) -> None:
    for f in data["feeds"]:
        cur.execute(_sql("upsert_feed.sql"), (f["url"], f["name"], f["language"]))

    for a in data["articles"]:
        cur.execute(_sql("upsert_article.sql"), (
            a["url"], a["title"], a["published_at"], a["full_text"], a["language"], a["feed_url"],
        ))

    for s in data["sentiments"]:
        cur.execute(_sql("upsert_sentiment.sql"), (
            s["article_url"], s["sentiment"], s["score"], s["confidence"], s["reasoning"], s["analyzed_at"],
        ))

    for m in data["company_mentions"]:
        cur.execute(_sql("upsert_company_mention.sql"), (m["article_url"], m["ticker"]))

    for m in data["sector_mentions"]:
        cur.execute(_sql("upsert_sector_mention.sql"), (m["article_url"], m["sector_name"]))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Loading source data...")
    companies = load_companies()
    print(f"  {len(companies)} companies")

    prices = load_stock_prices()
    print(f"  {len(prices)} stock price rows")

    print("Dumping Neo4j...")
    neo4j_data = load_neo4j()
    print(f"  {len(neo4j_data['feeds'])} feeds, {len(neo4j_data['articles'])} articles, "
          f"{len(neo4j_data['sentiments'])} sentiments")

    print("Writing to PostgreSQL...")
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_sql("schema.sql"))
            print("  Schema ready")

            upsert_companies(cur, companies)
            print(f"  Upserted {len(companies)} companies")

            known_tickers = set(companies["ticker"])
            prices_filtered = prices[prices["ticker"].isin(known_tickers)]
            upsert_stock_prices(cur, prices_filtered)
            print(f"  Upserted {len(prices_filtered)} stock price rows")

            upsert_neo4j_data(cur, neo4j_data)
            print(f"  Upserted {len(neo4j_data['feeds'])} feeds, "
                  f"{len(neo4j_data['articles'])} articles, "
                  f"{len(neo4j_data['sentiments'])} sentiments")

        conn.commit()

    print("Done.")


if __name__ == "__main__":
    main()
