INSERT INTO person_companies (normalized_name, ticker)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
