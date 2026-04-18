INSERT INTO feeds (url, name, language)
VALUES (%s, %s, %s)
ON CONFLICT (url) DO UPDATE SET
    name     = EXCLUDED.name,
    language = EXCLUDED.language;
