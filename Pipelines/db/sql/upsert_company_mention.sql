INSERT INTO article_company_mentions (article_url, ticker)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
