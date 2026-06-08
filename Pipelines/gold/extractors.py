import pandas as pd
from sqlalchemy.engine import Engine


def load_articles(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT
            a.url,
            a.title,
            a.published_at,
            a.full_text,
            a.language,
            a.feed_url,
            s.sentiment,
            s.score,
            s.confidence
        FROM articles a
        LEFT JOIN sentiment_scores s ON s.article_url = a.url
        WHERE a.published_at IS NOT NULL
        """,
        engine,
        parse_dates=["published_at"],
    )


def load_company_mentions(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT article_url, ticker FROM article_company_mentions",
        engine,
    )


def load_article_events(engine: Engine) -> pd.DataFrame:
    """Returns (article_url, event_type) for LLM-extracted event features."""
    return pd.read_sql(
        """
        SELECT ae.article_url, e.event_type
        FROM article_events ae
        JOIN events e ON e.fingerprint = ae.event_fingerprint
        WHERE e.event_type IS NOT NULL
        """,
        engine,
    )


def load_stock_prices(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT ticker, date, close, open, high, low, volume, change_pct
        FROM stock_prices
        ORDER BY ticker, date
        """,
        engine,
        parse_dates=["date"],
    )


def load_companies(engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        """
        SELECT ticker, company_name, sector, ceo, founded,
               headquarters, revenue, employees
        FROM companies
        """,
        engine,
    )


def load_events(engine: Engine) -> pd.DataFrame:
    """Returns one row per (event, company) pair."""
    return pd.read_sql(
        """
        SELECT
            e.fingerprint,
            e.event_type,
            e.event_date,
            e.first_seen,
            e.article_count,
            ec.ticker
        FROM events e
        LEFT JOIN event_companies ec ON ec.event_fingerprint = e.fingerprint
        """,
        engine,
        parse_dates=["event_date", "first_seen"],
    )
