"""Read-only access to the pipeline log SQLite file + live Postgres counts."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator, Optional

import pandas as pd
import psycopg2

SQLITE_PATH = os.getenv("PIPELINE_LOG_DB", "/var/log/pfa_bvc/pipeline_logs.db")


@contextmanager
def sqlite_conn() -> Iterator[sqlite3.Connection]:
    uri = f"file:{SQLITE_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        yield conn
    finally:
        conn.close()


def sqlite_available() -> bool:
    return os.path.exists(SQLITE_PATH)


def query_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    if not sqlite_available():
        return pd.DataFrame()
    try:
        with sqlite_conn() as conn:
            return pd.read_sql_query(sql, conn, params=params or None)
    except (sqlite3.OperationalError, pd.errors.DatabaseError):
        return pd.DataFrame()


# --- Postgres ---------------------------------------------------------------

def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "pfa_bvc"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
        connect_timeout=3,
    )


def pg_scalar(sql: str) -> Optional[int]:
    try:
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def stock_prices_today() -> Optional[int]:
    return pg_scalar(
        "SELECT COUNT(*) FROM stock_prices_daily "
        "WHERE scraped_at::date = CURRENT_DATE"
    )


def articles_total() -> Optional[int]:
    return pg_scalar("SELECT COUNT(*) FROM articles")


def pg_df(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        with _pg_conn() as conn:
            return pd.read_sql_query(sql, conn, params=params or None)
    except Exception:
        return pd.DataFrame()


def stock_prices_history(days: int = 30) -> pd.DataFrame:
    return pg_df(
        "SELECT ticker, libelle, cours, variation, scraped_at "
        "FROM stock_prices_daily "
        "WHERE scraped_at >= CURRENT_DATE - (%s * INTERVAL '1 day') "
        "ORDER BY scraped_at, ticker",
        (days,),
    )


def daily_scrape_coverage() -> pd.DataFrame:
    return pg_df(
        "SELECT scraped_at, COUNT(DISTINCT ticker) AS tickers_scraped "
        "FROM stock_prices_daily "
        "WHERE scraped_at >= CURRENT_DATE - (30 * INTERVAL '1 day') "
        "GROUP BY scraped_at ORDER BY scraped_at"
    )
