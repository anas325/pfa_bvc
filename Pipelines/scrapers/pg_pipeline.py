import os
from datetime import date
from pathlib import Path

import psycopg2

_SQL_DIR = Path(__file__).parent.parent / "db" / "sql"


def _sql(name: str) -> str:
    return (_SQL_DIR / name).read_text(encoding="utf-8")


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
        self.upsert_sql = _sql("upsert_stock_price_daily.sql")

    def close_spider(self, spider):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        self.cur.execute(
            self.upsert_sql,
            (
                item.get("ticker", "").strip(),
                item.get("libelle", "").strip(),
                item.get("cours", "").strip(),
                item.get("variation", "").strip(),
                date.today().isoformat(),
            ),
        )
        return item
