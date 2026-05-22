INSERT INTO event_companies (event_fingerprint, ticker)
VALUES (%s, %s)
ON CONFLICT DO NOTHING;
