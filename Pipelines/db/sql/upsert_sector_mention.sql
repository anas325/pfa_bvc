INSERT INTO article_sector_mentions (article_url, sector_name)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
