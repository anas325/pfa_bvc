import os
import sys
from datetime import date
from pathlib import Path

import psycopg2

# Ensure sibling packages (monitoring) are importable when Scrapy loads this module.
_PIPELINES_ROOT = Path(__file__).parent.parent
if str(_PIPELINES_ROOT) not in sys.path:
    sys.path.insert(0, str(_PIPELINES_ROOT))

from monitoring import PipelineLogger

_SQL_DIR = _PIPELINES_ROOT / "db" / "sql"


def _sql(name: str) -> str:
    return (_SQL_DIR / name).read_text(encoding="utf-8")


class PostgresStockPipeline:
    def open_spider(self, spider):
        self.log = PipelineLogger(f"stock_prices:{spider.name}")
        self.log.__enter__()
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("PG_HOST", "localhost"),
                port=int(os.getenv("PG_PORT", "5432")),
                dbname=os.getenv("PG_DB", "pfa_bvc"),
                user=os.getenv("PG_USER", "postgres"),
                password=os.getenv("PG_PASSWORD", "postgres"),
            )
            self.cur = self.conn.cursor()
            self.cur.execute(_sql("create_stock_prices_daily.sql"))
            self.conn.commit()
            self.upsert_sql = _sql("upsert_stock_price_daily.sql")
            self.log.event("postgres connection established", stage="setup")
        except Exception as e:
            self.log.__exit__(type(e), e, e.__traceback__)
            raise

    def close_spider(self, spider):
        exc_info = (None, None, None)
        try:
            self.conn.commit()
            self.cur.close()
            self.conn.close()
            self.log.event("spider finished", stage="close")
        except Exception as e:
            exc_info = (type(e), e, e.__traceback__)
            raise
        finally:
            self.log.__exit__(*exc_info)

    def process_item(self, item, spider):
        ticker = item.get("ticker", "").strip()
        try:
            self.cur.execute(
                self.upsert_sql,
                (
                    ticker,
                    item.get("libelle", "").strip(),
                    item.get("cours", "").strip(),
                    item.get("variation", "").strip(),
                    date.today().isoformat(),
                ),
            )
            self.log.increment_processed()
        except Exception as e:
            self.conn.rollback()
            self.log.increment_failed()
            self.log.event(
                f"upsert failed for {ticker}: {e}",
                level="error",
                stage="upsert",
                item_key=ticker,
            )
            raise
        return item
