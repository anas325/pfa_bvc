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
