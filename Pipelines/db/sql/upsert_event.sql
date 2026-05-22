INSERT INTO events (fingerprint, event_type, event_date, first_seen, article_count)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (fingerprint) DO UPDATE SET
    event_type    = EXCLUDED.event_type,
    event_date    = EXCLUDED.event_date,
    article_count = EXCLUDED.article_count;
