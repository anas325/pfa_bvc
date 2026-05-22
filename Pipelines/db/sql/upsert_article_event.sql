INSERT INTO article_events (article_url, event_fingerprint)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
