INSERT INTO people (normalized_name, name, role)
VALUES (%s, %s, %s)
ON CONFLICT (normalized_name) DO UPDATE SET
    name = EXCLUDED.name,
    role = EXCLUDED.role;
