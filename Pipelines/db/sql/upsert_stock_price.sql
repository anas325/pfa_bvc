INSERT INTO stock_prices (ticker, date, close, open, high, low, volume, change_pct)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker, date) DO UPDATE SET
    close      = EXCLUDED.close,
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    volume     = EXCLUDED.volume,
    change_pct = EXCLUDED.change_pct;
