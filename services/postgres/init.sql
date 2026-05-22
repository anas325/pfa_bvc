CREATE DATABASE airflow;

\connect pfa_bvc




CREATE TABLE IF NOT EXISTS stock_prices (
    id           SERIAL PRIMARY KEY,
    ticker       VARCHAR(20)  NOT NULL,
    libelle      VARCHAR(255),
    cours        VARCHAR(50),
    variation    VARCHAR(50),
    scraped_at   DATE         NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (ticker, scraped_at)
);

CREATE TABLE IF NOT EXISTS stock_prices_daily (
    id           SERIAL PRIMARY KEY,
    ticker       VARCHAR(20)  NOT NULL,
    libelle      VARCHAR(255),
    cours        VARCHAR(50),
    variation    VARCHAR(50),
    scraped_at   DATE         NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (ticker, scraped_at)
);

CREATE TABLE IF NOT EXISTS events (
    id            SERIAL PRIMARY KEY,
    fingerprint   VARCHAR(255) NOT NULL UNIQUE,
    event_type    VARCHAR(100),
    event_date    DATE,
    first_seen    TIMESTAMPTZ,
    article_count INTEGER      NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS people (
    id              SERIAL PRIMARY KEY,
    normalized_name VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(255),
    role            VARCHAR(255)
);

-- (Article)-[:COVERS]->(Event)
CREATE TABLE IF NOT EXISTS article_events (
    article_url       TEXT         NOT NULL,
    event_fingerprint VARCHAR(255) NOT NULL,
    PRIMARY KEY (article_url, event_fingerprint)
);

-- (Article)-[:MENTIONS_PERSON]->(Person)
CREATE TABLE IF NOT EXISTS article_people (
    article_url     TEXT         NOT NULL,
    normalized_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (article_url, normalized_name)
);

-- (Event)-[:INVOLVES]->(Company)
CREATE TABLE IF NOT EXISTS event_companies (
    event_fingerprint VARCHAR(255) NOT NULL,
    ticker            VARCHAR(20)  NOT NULL,
    PRIMARY KEY (event_fingerprint, ticker)
);

-- (Person)-[:ASSOCIATED_WITH]->(Company)
CREATE TABLE IF NOT EXISTS person_companies (
    normalized_name VARCHAR(255) NOT NULL,
    ticker          VARCHAR(20)  NOT NULL,
    PRIMARY KEY (normalized_name, ticker)
);
