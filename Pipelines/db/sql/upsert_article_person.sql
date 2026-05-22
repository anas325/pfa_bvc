INSERT INTO article_people (article_url, normalized_name)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
