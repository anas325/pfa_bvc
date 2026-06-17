"""Read-only access to the Postgres ``gold`` schema for the analytics dashboard.

All loaders are cached and degrade gracefully to an empty DataFrame on any error so
the UI never shows a traceback when a table is empty or Postgres is unreachable.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

@st.cache_resource
def get_engine() -> Engine:
    user = os.getenv("PG_USER", "postgres")
    pwd = os.getenv("PG_PASSWORD", "postgres")
    host = os.getenv("PG_HOST", "postgres")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "pfa_bvc")
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})


def _query(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        with get_engine().connect() as conn:
            return pd.read_sql_query(text(sql), conn, params=params or {})
    except Exception as exc:  # noqa: BLE001 — surfaced in UI, never crash
        st.session_state["_last_db_error"] = str(exc)
        return pd.DataFrame()


def last_error() -> str | None:
    return st.session_state.get("_last_db_error")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def companies() -> pd.DataFrame:
    return _query(
        """
        SELECT ticker, company_name, sector, ceo, founded, headquarters,
               revenue, employees, total_news_count, avg_sentiment_all_time,
               top_event_type, first_article_date, last_article_date
        FROM gold.company_dim
        ORDER BY company_name
        """
    )


@st.cache_data(ttl=300)
def daily_signals(ticker: str, days: int = 365) -> pd.DataFrame:
    return _query(
        """
        SELECT *
        FROM gold.daily_signals
        WHERE ticker = :ticker
          AND date >= (SELECT MAX(date) FROM gold.daily_signals) - (:days * INTERVAL '1 day')
        ORDER BY date
        """,
        {"ticker": ticker, "days": days},
    )


@st.cache_data(ttl=300)
def weekly(ticker: str) -> pd.DataFrame:
    return _query(
        "SELECT * FROM gold.sentiment_weekly WHERE ticker = :ticker ORDER BY week_start",
        {"ticker": ticker},
    )


@st.cache_data(ttl=300)
def monthly(ticker: str) -> pd.DataFrame:
    return _query(
        "SELECT * FROM gold.sentiment_monthly WHERE ticker = :ticker ORDER BY month_start",
        {"ticker": ticker},
    )


@st.cache_data(ttl=300)
def market_latest() -> pd.DataFrame:
    """Latest available row per ticker, joined with company profile."""
    return _query(
        """
        SELECT DISTINCT ON (ds.ticker)
               ds.ticker, cd.company_name, cd.sector,
               ds.date, ds.close, ds.change_pct, ds.daily_return,
               ds.volume, ds.news_count, ds.avg_sentiment,
               ds.avg_sentiment_r7d, ds.positive_ratio, ds.negative_ratio
        FROM gold.daily_signals ds
        JOIN gold.company_dim cd ON cd.ticker = ds.ticker
        ORDER BY ds.ticker, ds.date DESC
        """
    )


@st.cache_data(ttl=300)
def market_news_volume(days: int = 90) -> pd.DataFrame:
    """Market-wide daily news count and average sentiment over time."""
    return _query(
        """
        SELECT date,
               SUM(news_count) AS news_count,
               AVG(NULLIF(avg_sentiment, 0)) AS avg_sentiment
        FROM gold.daily_signals
        WHERE has_news
          AND date >= (SELECT MAX(date) FROM gold.daily_signals) - (:days * INTERVAL '1 day')
        GROUP BY date
        ORDER BY date
        """,
        {"days": days},
    )


@st.cache_data(ttl=300)
def sector_sentiment(days: int = 90) -> pd.DataFrame:
    """Average sentiment + news volume by sector over a recent window."""
    return _query(
        """
        SELECT cd.sector,
               AVG(ds.avg_sentiment) AS avg_sentiment,
               SUM(ds.news_count)    AS news_count,
               COUNT(DISTINCT ds.ticker) AS companies
        FROM gold.daily_signals ds
        JOIN gold.company_dim cd ON cd.ticker = ds.ticker
        WHERE ds.has_news
          AND cd.sector IS NOT NULL
          AND ds.date >= (SELECT MAX(date) FROM gold.daily_signals) - (:days * INTERVAL '1 day')
        GROUP BY cd.sector
        ORDER BY news_count DESC NULLS LAST
        """,
        {"days": days},
    )


@st.cache_data(ttl=300)
def sentiment_return_pairs(days: int = 365) -> pd.DataFrame:
    """Per (ticker, day) sentiment vs same-day and next-day return, news days only."""
    return _query(
        """
        SELECT ds.ticker, cd.sector, ds.date,
               ds.avg_sentiment, ds.news_count,
               ds.daily_return, ds.next_day_return
        FROM gold.daily_signals ds
        JOIN gold.company_dim cd ON cd.ticker = ds.ticker
        WHERE ds.has_news
          AND ds.avg_sentiment IS NOT NULL
          AND ds.date >= (SELECT MAX(date) FROM gold.daily_signals) - (:days * INTERVAL '1 day')
        """,
        {"days": days},
    )


# Event-type one-hot columns present on gold.daily_signals
LLM_EVENT_TYPES = [
    "capital_operation", "debt_issuance", "dividend_announcement", "earnings_release",
    "economic_indicator", "ipo_listing", "leadership_change", "ma_deal",
    "market_data", "project_contract", "regulatory_action", "strategic_plan",
]


@st.cache_data(ttl=300)
def event_impact() -> pd.DataFrame:
    """Average next-day return on days an event type was present vs absent."""
    selects = []
    for ev in LLM_EVENT_TYPES:
        col = f"llm_evt_{ev}"
        selects.append(
            f"AVG(next_day_return) FILTER (WHERE {col} > 0) AS \"{ev}__present\""
        )
        selects.append(
            f"AVG(next_day_return) FILTER (WHERE {col} = 0 OR {col} IS NULL) AS \"{ev}__absent\""
        )
        selects.append(f"COUNT(*) FILTER (WHERE {col} > 0) AS \"{ev}__n\"")
    sql = (
        "SELECT " + ", ".join(selects)
        + " FROM gold.daily_signals WHERE next_day_return IS NOT NULL"
    )
    return _query(sql)


@st.cache_data(ttl=300)
def events(limit: int = 500, event_type: str | None = None) -> pd.DataFrame:
    where = "WHERE ef.event_date IS NOT NULL"
    params: dict = {"limit": limit}
    if event_type and event_type != "All":
        where += " AND ef.event_type = :etype"
        params["etype"] = event_type
    return _query(
        f"""
        SELECT ef.fingerprint, ef.event_type, ef.event_date, ef.first_seen,
               ef.article_count,
               STRING_AGG(DISTINCT ecb.ticker, ', ') AS tickers
        FROM gold.event_facts ef
        LEFT JOIN gold.event_company_bridge ecb ON ecb.fingerprint = ef.fingerprint
        {where}
        GROUP BY ef.fingerprint, ef.event_type, ef.event_date, ef.first_seen, ef.article_count
        ORDER BY ef.event_date DESC
        LIMIT :limit
        """,
        params,
    )


@st.cache_data(ttl=300)
def events_by_type_over_time(freq: str = "month") -> pd.DataFrame:
    return _query(
        """
        SELECT date_trunc(:freq, event_date)::date AS period,
               event_type,
               COUNT(*) AS n
        FROM gold.event_facts
        WHERE event_date IS NOT NULL
        GROUP BY period, event_type
        ORDER BY period
        """,
        {"freq": freq},
    )


@st.cache_data(ttl=300)
def most_active_companies(limit: int = 15) -> pd.DataFrame:
    return _query(
        """
        SELECT ecb.ticker, cd.company_name, cd.sector,
               COUNT(DISTINCT ecb.fingerprint) AS event_count
        FROM gold.event_company_bridge ecb
        JOIN gold.company_dim cd ON cd.ticker = ecb.ticker
        GROUP BY ecb.ticker, cd.company_name, cd.sector
        ORDER BY event_count DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )


@st.cache_data(ttl=300)
def ticker_events(ticker: str) -> pd.DataFrame:
    return _query(
        """
        SELECT ef.event_type, ef.event_date, ef.article_count
        FROM gold.event_company_bridge ecb
        JOIN gold.event_facts ef ON ef.fingerprint = ecb.fingerprint
        WHERE ecb.ticker = :ticker AND ef.event_date IS NOT NULL
        ORDER BY ef.event_date
        """,
        {"ticker": ticker},
    )


@st.cache_data(ttl=300)
def commodities() -> pd.DataFrame:
    """Latest snapshot per commodity/forex asset."""
    return _query(
        """
        SELECT DISTINCT ON (asset_key)
               asset_key, name, category, date, close,
               daily_return, return_r7d, volatility_r7d
        FROM gold.commodity_daily
        ORDER BY asset_key, date DESC
        """
    )


@st.cache_data(ttl=300)
def commodity_history(asset_key: str, days: int = 180) -> pd.DataFrame:
    return _query(
        """
        SELECT date, close, daily_return
        FROM gold.commodity_daily
        WHERE asset_key = :asset_key
          AND date >= (SELECT MAX(date) FROM gold.commodity_daily) - (:days * INTERVAL '1 day')
        ORDER BY date
        """,
        {"asset_key": asset_key, "days": days},
    )
