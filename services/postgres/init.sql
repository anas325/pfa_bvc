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
