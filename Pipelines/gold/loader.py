import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS gold_daily_signals (
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
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS gold_company_dim (
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

CREATE TABLE IF NOT EXISTS gold_event_facts (
    fingerprint     TEXT        PRIMARY KEY,
    event_type      TEXT,
    event_date      DATE,
    first_seen      TIMESTAMPTZ,
    article_count   INT         NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS gold_event_company_bridge (
    fingerprint     TEXT        NOT NULL,
    ticker          TEXT        NOT NULL,
    PRIMARY KEY (fingerprint, ticker)
);

CREATE TABLE IF NOT EXISTS gold_sentiment_weekly (
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
    PRIMARY KEY (ticker, week_start)
);

CREATE TABLE IF NOT EXISTS gold_sentiment_monthly (
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
    PRIMARY KEY (ticker, month_start)
);
"""

_GOLD_TABLES = [
    "gold_daily_signals",
    "gold_company_dim",
    "gold_event_facts",
    "gold_event_company_bridge",
    "gold_sentiment_weekly",
    "gold_sentiment_monthly",
]


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_SCHEMA_SQL))


def full_refresh(engine: Engine, table_name: str, df: pd.DataFrame) -> None:
    if table_name not in _GOLD_TABLES:
        raise ValueError(f"Unknown gold table: {table_name}")
    if df.empty:
        print(f"  [{table_name}] skipped — no data")
        return
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        df.to_sql(table_name, conn, if_exists="append", index=False, method="multi")
    print(f"  [{table_name}] {len(df):,} rows written")


def run_all(
    engine: Engine,
    signals_df: pd.DataFrame,
    company_dim_df: pd.DataFrame,
    event_facts_df: pd.DataFrame,
    event_bridge_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
) -> None:
    full_refresh(engine, "gold_daily_signals", signals_df)
    full_refresh(engine, "gold_company_dim", company_dim_df)
    full_refresh(engine, "gold_event_facts", event_facts_df)
    full_refresh(engine, "gold_event_company_bridge", event_bridge_df)
    full_refresh(engine, "gold_sentiment_weekly", weekly_df)
    full_refresh(engine, "gold_sentiment_monthly", monthly_df)
