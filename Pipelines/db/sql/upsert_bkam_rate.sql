INSERT INTO bkam_rates (rate_date, currency, country, unit, buy_rate, sell_rate)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (rate_date, currency) DO UPDATE SET
    country   = EXCLUDED.country,
    unit      = EXCLUDED.unit,
    buy_rate  = EXCLUDED.buy_rate,
    sell_rate = EXCLUDED.sell_rate;
