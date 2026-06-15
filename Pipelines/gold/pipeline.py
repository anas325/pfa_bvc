import logging
import os

from sqlalchemy import create_engine

from gold import extractors, features, loader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def _pg_engine():
    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DB", "pfa_bvc")
    user = os.environ.get("PG_USER", "postgres")
    password = os.environ.get("PG_PASSWORD", "postgres")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def run() -> None:
    engine = _pg_engine()

    log.info("Extracting silver layer data from PostgreSQL...")
    articles_df = extractors.load_articles(engine)
    mentions_df = extractors.load_company_mentions(engine)
    article_events_df = extractors.load_article_events(engine)
    stocks_df = extractors.load_stock_prices(engine)
    companies_df = extractors.load_companies(engine)
    events_raw_df = extractors.load_events(engine)
    log.info(
        "Loaded: %d articles, %d mentions, %d article-events, %d stock rows, %d companies, %d event rows",
        len(articles_df),
        len(mentions_df),
        len(article_events_df),
        len(stocks_df),
        len(companies_df),
        len(events_raw_df),
    )

    log.info("Mapping articles to company tickers...")
    mapped_df = features.map_entities(articles_df, companies_df, mentions_df)
    log.info("Mapped: %d article-ticker rows", len(mapped_df))

    log.info("Enriching articles (event flags + sentiment fallback)...")
    enriched_df = features.enrich_articles(mapped_df, article_events_df)

    log.info("Building daily features...")
    features_df = features.build_features(enriched_df)
    log.info("Features: %d (ticker, date) rows", len(features_df))

    log.info("Joining features onto full stock price history...")
    signals_df = features.build_daily_signals(features_df, stocks_df)
    log.info("Daily signals: %d rows (%d with news)", len(signals_df), signals_df["has_news"].sum())

    log.info("Building company dimension...")
    company_dim_df = features.build_company_dim(companies_df, enriched_df)

    log.info("Building BI aggregations...")
    weekly_df, monthly_df = features.build_bi_aggregations(features_df)
    log.info("Weekly: %d rows | Monthly: %d rows", len(weekly_df), len(monthly_df))

    log.info("Building event tables...")
    event_facts_df, event_bridge_df = features.build_event_tables(events_raw_df)
    log.info("Events: %d facts, %d company links", len(event_facts_df), len(event_bridge_df))

    log.info("Building commodity features...")
    commodity_df = features.build_commodity_features(engine)
    log.info("Commodity daily: %d rows across %d assets", len(commodity_df), commodity_df["asset_key"].nunique() if not commodity_df.empty else 0)

    log.info("Ensuring gold schema exists...")
    loader.ensure_schema(engine)

    log.info("Writing gold tables (full refresh)...")
    loader.run_all(
        engine,
        signals_df,
        company_dim_df,
        event_facts_df,
        event_bridge_df,
        weekly_df,
        monthly_df,
        commodity_df,
    )

    log.info("Gold layer refresh complete.")


if __name__ == "__main__":
    run()
