CREATE TABLE IF NOT EXISTS companies (
    ticker          TEXT PRIMARY KEY,
    company_name    TEXT,
    sector          TEXT,
    parent          TEXT,
    description     TEXT,
    ceo             TEXT,
    founded         INT,
    headquarters    TEXT,
    revenue         TEXT,
    employees       INT,
    stock_exchange  TEXT,
    siege_social    TEXT
);

CREATE TABLE IF NOT EXISTS stock_prices (
    ticker      TEXT REFERENCES companies(ticker),
    date        DATE,
    close       NUMERIC,
    open        NUMERIC,
    high        NUMERIC,
    low         NUMERIC,
    volume      NUMERIC,
    change_pct  NUMERIC,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS stock_prices_daily (
    id           SERIAL PRIMARY KEY,
    ticker       TEXT         NOT NULL,
    libelle      TEXT,
    cours        TEXT,
    variation    TEXT,
    scraped_at   DATE         NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (ticker, scraped_at)
);

CREATE TABLE IF NOT EXISTS feeds (
    url         TEXT PRIMARY KEY,
    name        TEXT,
    language    TEXT
);

CREATE TABLE IF NOT EXISTS articles (
    url             TEXT PRIMARY KEY,
    title           TEXT,
    published_at    TIMESTAMPTZ,
    full_text       TEXT,
    language        TEXT,
    feed_url        TEXT REFERENCES feeds(url)
);

CREATE TABLE IF NOT EXISTS sentiment_scores (
    article_url TEXT PRIMARY KEY REFERENCES articles(url),
    sentiment   TEXT,
    score       NUMERIC,
    confidence  NUMERIC,
    reasoning   TEXT,
    analyzed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS article_company_mentions (
    article_url TEXT REFERENCES articles(url),
    ticker      TEXT REFERENCES companies(ticker),
    PRIMARY KEY (article_url, ticker)
);

CREATE TABLE IF NOT EXISTS article_sector_mentions (
    article_url TEXT,
    sector_name TEXT,
    PRIMARY KEY (article_url, sector_name)
);

CREATE TABLE IF NOT EXISTS events (
    fingerprint   TEXT PRIMARY KEY,
    event_type    TEXT,
    event_date    DATE,
    first_seen    TIMESTAMPTZ,
    article_count INT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS people (
    normalized_name TEXT PRIMARY KEY,
    name            TEXT,
    role            TEXT
);

CREATE TABLE IF NOT EXISTS article_events (
    article_url       TEXT REFERENCES articles(url),
    event_fingerprint TEXT REFERENCES events(fingerprint),
    PRIMARY KEY (article_url, event_fingerprint)
);

CREATE TABLE IF NOT EXISTS article_people (
    article_url     TEXT REFERENCES articles(url),
    normalized_name TEXT REFERENCES people(normalized_name),
    PRIMARY KEY (article_url, normalized_name)
);

CREATE TABLE IF NOT EXISTS event_companies (
    event_fingerprint TEXT REFERENCES events(fingerprint),
    ticker            TEXT REFERENCES companies(ticker),
    PRIMARY KEY (event_fingerprint, ticker)
);

CREATE TABLE IF NOT EXISTS person_companies (
    normalized_name TEXT REFERENCES people(normalized_name),
    ticker          TEXT REFERENCES companies(ticker),
    PRIMARY KEY (normalized_name, ticker)
);

CREATE TABLE IF NOT EXISTS commodities (
    asset_key   TEXT,
    date        DATE,
    ticker      TEXT       NOT NULL,
    name        TEXT,
    category    TEXT,
    open        NUMERIC,
    high        NUMERIC,
    low         NUMERIC,
    close       NUMERIC,
    adj_close   NUMERIC,
    volume      NUMERIC,
    PRIMARY KEY (asset_key, date)
);


CREATE TABLE IF NOT EXISTS bkam_rates (
    id         SERIAL PRIMARY KEY,
    rate_date  DATE         NOT NULL,
    currency   VARCHAR(10)  NOT NULL,
    country    VARCHAR(100),
    unit       INTEGER,
    buy_rate   NUMERIC(12,4),
    sell_rate  NUMERIC(12,4),
    scraped_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (rate_date, currency)
);

-- ---------------------------------------------------------------------------
-- Gold schema — analytics-ready tables (managed by Pipelines/gold/loader.py)
-- Full refresh on every DAG run; do not modify manually.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS gold;

-- Parents first (no FKs)
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

-- Children (reference parents via FK)
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
    -- base sentiment/event features
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
    -- rolling 3-day
    news_count_r3d                      NUMERIC(8,4),
    avg_sentiment_r3d                   NUMERIC(8,5),
    std_sentiment_r3d                   NUMERIC(8,5),
    positive_ratio_r3d                  NUMERIC(8,5),
    negative_ratio_r3d                  NUMERIC(8,5),
    event_ma_r3d                        NUMERIC(8,4),
    event_earnings_r3d                  NUMERIC(8,4),
    event_management_r3d                NUMERIC(8,4),
    event_legal_r3d                     NUMERIC(8,4),
    -- rolling 7-day
    news_count_r7d                      NUMERIC(8,4),
    avg_sentiment_r7d                   NUMERIC(8,5),
    std_sentiment_r7d                   NUMERIC(8,5),
    positive_ratio_r7d                  NUMERIC(8,5),
    negative_ratio_r7d                  NUMERIC(8,5),
    event_ma_r7d                        NUMERIC(8,4),
    event_earnings_r7d                  NUMERIC(8,4),
    event_management_r7d                NUMERIC(8,4),
    event_legal_r7d                     NUMERIC(8,4),
    -- LLM event types
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
    -- LLM event rolling 3-day
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
    -- LLM event rolling 7-day
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
