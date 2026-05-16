INSERT INTO commodities (asset_key, date, ticker, name, category, open, high, low, close, adj_close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (asset_key, date) DO UPDATE SET
    open      = EXCLUDED.open,
    high      = EXCLUDED.high,
    low       = EXCLUDED.low,
    close     = EXCLUDED.close,
    adj_close = EXCLUDED.adj_close,
    volume    = EXCLUDED.volume;
