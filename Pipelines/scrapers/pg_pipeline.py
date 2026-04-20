import os
from datetime import date

import psycopg2


class PostgresStockPipeline:
    def open_spider(self, spider):
        self.conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432")),
            dbname=os.getenv("PG_DB", "pfa_bvc"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", "postgres"),
        )
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        self.cur.execute(
            """
            INSERT INTO stock_prices (ticker, libelle, cours, variation, scraped_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticker, scraped_at) DO UPDATE
                SET cours     = EXCLUDED.cours,
                    variation = EXCLUDED.variation,
                    libelle   = EXCLUDED.libelle
            """,
            (
                item.get("ticker", "").strip(),
                item.get("libelle", "").strip(),
                item.get("cours", "").strip(),
                item.get("variation", "").strip(),
                date.today().isoformat(),
            ),
        )
        return item
