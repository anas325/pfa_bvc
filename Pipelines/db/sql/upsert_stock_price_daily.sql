INSERT INTO stock_prices_daily (ticker, libelle, cours, variation, scraped_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (ticker, scraped_at) DO UPDATE SET
    cours     = EXCLUDED.cours,
    variation = EXCLUDED.variation,
    libelle   = EXCLUDED.libelle;
