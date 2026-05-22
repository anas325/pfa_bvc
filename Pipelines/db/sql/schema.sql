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
