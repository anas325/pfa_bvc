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
