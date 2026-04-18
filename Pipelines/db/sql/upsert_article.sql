INSERT INTO articles (url, title, published_at, full_text, language, feed_url)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (url) DO UPDATE SET
    title        = EXCLUDED.title,
    published_at = EXCLUDED.published_at,
    full_text    = EXCLUDED.full_text,
    language     = EXCLUDED.language,
    feed_url     = EXCLUDED.feed_url;
