import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.company_dim (
    ticker                  TEXT        PRIMARY KEY,
    company_name            TEXT,
    sector                  TEXT,
    ceo                     TEXT,
    founded                 INT,
    headquarters            TEXT,
    revenue                 TEXT,
    employees               INT,
    total_news_count        INT         NOT NULL DEFAULT 0,
    avg_sentiment_all_time  NUMERIC(8,5),
    top_event_type          TEXT,
    first_article_date      DATE,
    last_article_date       DATE
);

CREATE TABLE IF NOT EXISTS gold.event_facts (
    fingerprint     TEXT        PRIMARY KEY,
    event_type      TEXT,
    event_date      DATE,
    first_seen      TIMESTAMPTZ,
    article_count   INT         NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS gold.event_company_bridge (
    fingerprint     TEXT        NOT NULL,
    ticker          TEXT        NOT NULL,
    PRIMARY KEY (fingerprint, ticker),
    CONSTRAINT fk_ecb_event    FOREIGN KEY (fingerprint) REFERENCES gold.event_facts(fingerprint),
    CONSTRAINT fk_ecb_company  FOREIGN KEY (ticker)      REFERENCES gold.company_dim(ticker)
);

CREATE TABLE IF NOT EXISTS gold.daily_signals (
    ticker                              TEXT        NOT NULL,
    date                                DATE        NOT NULL,
    close                               NUMERIC,
    open                                NUMERIC,
    high                                NUMERIC,
    low                                 NUMERIC,
    volume                              NUMERIC,
    change_pct                          NUMERIC,
    daily_return                        NUMERIC,
    next_day_return                     NUMERIC,
    direction                           SMALLINT,
    has_news                            BOOLEAN     NOT NULL DEFAULT FALSE,
    news_count                          SMALLINT,
    avg_sentiment                       NUMERIC(8,5),
    std_sentiment                       NUMERIC(8,5),
    positive_count                      SMALLINT,
    negative_count                      SMALLINT,
    event_ma                            SMALLINT,
    event_earnings                      SMALLINT,
    event_management                    SMALLINT,
    event_legal                         SMALLINT,
    negated_count                       SMALLINT,
    speculative_count                   SMALLINT,
    positive_ratio                      NUMERIC(8,5),
    negative_ratio                      NUMERIC(8,5),
    news_count_r3d                      NUMERIC(8,4),
    avg_sentiment_r3d                   NUMERIC(8,5),
    std_sentiment_r3d                   NUMERIC(8,5),
    positive_ratio_r3d                  NUMERIC(8,5),
    negative_ratio_r3d                  NUMERIC(8,5),
    event_ma_r3d                        NUMERIC(8,4),
    event_earnings_r3d                  NUMERIC(8,4),
    event_management_r3d                NUMERIC(8,4),
    event_legal_r3d                     NUMERIC(8,4),
    news_count_r7d                      NUMERIC(8,4),
    avg_sentiment_r7d                   NUMERIC(8,5),
    std_sentiment_r7d                   NUMERIC(8,5),
    positive_ratio_r7d                  NUMERIC(8,5),
    negative_ratio_r7d                  NUMERIC(8,5),
    event_ma_r7d                        NUMERIC(8,4),
    event_earnings_r7d                  NUMERIC(8,4),
    event_management_r7d                NUMERIC(8,4),
    event_legal_r7d                     NUMERIC(8,4),
    llm_evt_capital_operation           SMALLINT,
    llm_evt_debt_issuance               SMALLINT,
    llm_evt_dividend_announcement       SMALLINT,
    llm_evt_earnings_release            SMALLINT,
    llm_evt_economic_indicator          SMALLINT,
    llm_evt_ipo_listing                 SMALLINT,
    llm_evt_leadership_change           SMALLINT,
    llm_evt_ma_deal                     SMALLINT,
    llm_evt_market_data                 SMALLINT,
    llm_evt_other                       SMALLINT,
    llm_evt_project_contract            SMALLINT,
    llm_evt_regulatory_action           SMALLINT,
    llm_evt_strategic_plan              SMALLINT,
    llm_evt_capital_operation_r3d       NUMERIC(8,4),
    llm_evt_debt_issuance_r3d           NUMERIC(8,4),
    llm_evt_dividend_announcement_r3d   NUMERIC(8,4),
    llm_evt_earnings_release_r3d        NUMERIC(8,4),
    llm_evt_economic_indicator_r3d      NUMERIC(8,4),
    llm_evt_ipo_listing_r3d             NUMERIC(8,4),
    llm_evt_leadership_change_r3d       NUMERIC(8,4),
    llm_evt_ma_deal_r3d                 NUMERIC(8,4),
    llm_evt_market_data_r3d             NUMERIC(8,4),
    llm_evt_other_r3d                   NUMERIC(8,4),
    llm_evt_project_contract_r3d        NUMERIC(8,4),
    llm_evt_regulatory_action_r3d       NUMERIC(8,4),
    llm_evt_strategic_plan_r3d          NUMERIC(8,4),
    llm_evt_capital_operation_r7d       NUMERIC(8,4),
    llm_evt_debt_issuance_r7d           NUMERIC(8,4),
    llm_evt_dividend_announcement_r7d   NUMERIC(8,4),
    llm_evt_earnings_release_r7d        NUMERIC(8,4),
    llm_evt_economic_indicator_r7d      NUMERIC(8,4),
    llm_evt_ipo_listing_r7d             NUMERIC(8,4),
    llm_evt_leadership_change_r7d       NUMERIC(8,4),
    llm_evt_ma_deal_r7d                 NUMERIC(8,4),
    llm_evt_market_data_r7d             NUMERIC(8,4),
    llm_evt_other_r7d                   NUMERIC(8,4),
    llm_evt_project_contract_r7d        NUMERIC(8,4),
    llm_evt_regulatory_action_r7d       NUMERIC(8,4),
    llm_evt_strategic_plan_r7d          NUMERIC(8,4),
    PRIMARY KEY (ticker, date),
    CONSTRAINT fk_ds_company FOREIGN KEY (ticker) REFERENCES gold.company_dim(ticker)
);

CREATE TABLE IF NOT EXISTS gold.sentiment_weekly (
    ticker                          TEXT        NOT NULL,
    week_start                      DATE        NOT NULL,
    news_count                      INT,
    avg_sentiment                   NUMERIC(8,5),
    std_sentiment                   NUMERIC(8,5),
    positive_ratio                  NUMERIC(8,5),
    negative_ratio                  NUMERIC(8,5),
    event_ma                        INT,
    event_earnings                  INT,
    event_management                INT,
    event_legal                     INT,
    llm_evt_capital_operation       INT,
    llm_evt_debt_issuance           INT,
    llm_evt_dividend_announcement   INT,
    llm_evt_earnings_release        INT,
    llm_evt_economic_indicator      INT,
    llm_evt_ipo_listing             INT,
    llm_evt_leadership_change       INT,
    llm_evt_ma_deal                 INT,
    llm_evt_market_data             INT,
    llm_evt_other                   INT,
    llm_evt_project_contract        INT,
    llm_evt_regulatory_action       INT,
    llm_evt_strategic_plan          INT,
    PRIMARY KEY (ticker, week_start),
    CONSTRAINT fk_sw_company FOREIGN KEY (ticker) REFERENCES gold.company_dim(ticker)
);

CREATE TABLE IF NOT EXISTS gold.sentiment_monthly (
    ticker                          TEXT        NOT NULL,
    month_start                     DATE        NOT NULL,
    news_count                      INT,
    avg_sentiment                   NUMERIC(8,5),
    std_sentiment                   NUMERIC(8,5),
    positive_ratio                  NUMERIC(8,5),
    negative_ratio                  NUMERIC(8,5),
    event_ma                        INT,
    event_earnings                  INT,
    event_management                INT,
    event_legal                     INT,
    llm_evt_capital_operation       INT,
    llm_evt_debt_issuance           INT,
    llm_evt_dividend_announcement   INT,
    llm_evt_earnings_release        INT,
    llm_evt_economic_indicator      INT,
    llm_evt_ipo_listing             INT,
    llm_evt_leadership_change       INT,
    llm_evt_ma_deal                 INT,
    llm_evt_market_data             INT,
    llm_evt_other                   INT,
    llm_evt_project_contract        INT,
    llm_evt_regulatory_action       INT,
    llm_evt_strategic_plan          INT,
    PRIMARY KEY (ticker, month_start),
    CONSTRAINT fk_sm_company FOREIGN KEY (ticker) REFERENCES gold.company_dim(ticker)
);

CREATE TABLE IF NOT EXISTS gold.commodity_daily (
    asset_key       TEXT        NOT NULL,
    date            DATE        NOT NULL,
    name            TEXT,
    category        TEXT,
    close           NUMERIC,
    daily_return    NUMERIC,
    close_r3d       NUMERIC,
    close_r7d       NUMERIC,
    return_r3d      NUMERIC,
    return_r7d      NUMERIC,
    volatility_r7d  NUMERIC,
    PRIMARY KEY (asset_key, date)
);
"""

# Truncate order: children before parents (respects FK constraints)
_TRUNCATE_ORDER = [
    "gold.daily_signals",
    "gold.sentiment_weekly",
    "gold.sentiment_monthly",
    "gold.event_company_bridge",
    "gold.event_facts",
    "gold.company_dim",
    "gold.commodity_daily",
]

# Insert order: parents before children
_INSERT_ORDER = [
    "gold.company_dim",
    "gold.event_facts",
    "gold.event_company_bridge",
    "gold.daily_signals",
    "gold.sentiment_weekly",
    "gold.sentiment_monthly",
    "gold.commodity_daily",
]


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_SCHEMA_SQL))


def full_refresh(engine: Engine, table_name: str, df: pd.DataFrame) -> None:
    if table_name not in _INSERT_ORDER:
        raise ValueError(f"Unknown gold table: {table_name}")
    if df.empty:
        print(f"  [{table_name}] skipped — no data")
        return
    # pandas to_sql needs the schema and table name separated
    schema, tbl = table_name.split(".")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        df.to_sql(tbl, conn, schema=schema, if_exists="append", index=False, method="multi")
    print(f"  [{table_name}] {len(df):,} rows written")


def truncate_all(engine: Engine) -> None:
    """Truncates all gold tables in one statement (required for FK-constrained tables)."""
    tables = ", ".join(_TRUNCATE_ORDER)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE"))


def run_all(
    engine: Engine,
    signals_df: pd.DataFrame,
    company_dim_df: pd.DataFrame,
    event_facts_df: pd.DataFrame,
    event_bridge_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    commodity_df: pd.DataFrame,
) -> None:
    truncate_all(engine)
    schema = "gold"

    def _insert(tbl: str, df: pd.DataFrame) -> None:
        if df.empty:
            print(f"  [gold.{tbl}] skipped — no data")
            return
        with engine.begin() as conn:
            df.to_sql(tbl, conn, schema=schema, if_exists="append", index=False, method="multi")
        print(f"  [gold.{tbl}] {len(df):,} rows written")

    _insert("company_dim", company_dim_df)
    _insert("event_facts", event_facts_df)
    _insert("event_company_bridge", event_bridge_df)
    _insert("daily_signals", signals_df)
    _insert("sentiment_weekly", weekly_df)
    _insert("sentiment_monthly", monthly_df)
    _insert("commodity_daily", commodity_df)
