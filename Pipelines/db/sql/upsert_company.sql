INSERT INTO companies (ticker, company_name, sector, parent, description, ceo,
                       founded, headquarters, revenue, employees, stock_exchange, siege_social)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker) DO UPDATE SET
    company_name   = EXCLUDED.company_name,
    sector         = COALESCE(EXCLUDED.sector,         companies.sector),
    parent         = COALESCE(EXCLUDED.parent,         companies.parent),
    description    = COALESCE(EXCLUDED.description,    companies.description),
    ceo            = COALESCE(EXCLUDED.ceo,            companies.ceo),
    founded        = COALESCE(EXCLUDED.founded,        companies.founded),
    headquarters   = COALESCE(EXCLUDED.headquarters,   companies.headquarters),
    revenue        = COALESCE(EXCLUDED.revenue,        companies.revenue),
    employees      = COALESCE(EXCLUDED.employees,      companies.employees),
    stock_exchange = COALESCE(EXCLUDED.stock_exchange, companies.stock_exchange),
    siege_social   = COALESCE(EXCLUDED.siege_social,   companies.siege_social);
