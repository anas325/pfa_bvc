import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from scrapers.misc import currencies

BATCH_SIZE = 500

_PIPELINES_ROOT = Path(__file__).parent.parent
if str(_PIPELINES_ROOT) not in sys.path:
    sys.path.insert(0, str(_PIPELINES_ROOT))

from monitoring import PipelineLogger

_SQL_DIR = _PIPELINES_ROOT / "db" / "sql"


def _sql(name: str) -> str:
    return (_SQL_DIR / name).read_text(encoding="utf-8")


def _parse_rate(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def _parse_unit(value) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


class BkamPostgresPipeline:
    def open_spider(self, spider):
        self.log = PipelineLogger(f"bkam_rates:{spider.name}")
        self.log.__enter__()
        self.shorthand_dict = currencies.shorthand
        self._batch = []
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("PG_HOST", "localhost"),
                port=int(os.getenv("PG_PORT", "5432")),
                dbname=os.getenv("PG_DB", "pfa_bvc"),
                user=os.getenv("PG_USER", "postgres"),
                password=os.getenv("PG_PASSWORD", "postgres"),
            )
            self.cur = self.conn.cursor()
            self.cur.execute(_sql("create_bkam_rates.sql"))
            self.conn.commit()
            self.upsert_sql = _sql("upsert_bkam_rate.sql")
            self.log.event("postgres connection established", stage="setup")
        except Exception as e:
            self.log.__exit__(type(e), e, e.__traceback__)
            raise

    def _flush(self):
        if not self._batch:
            return
        try:
            psycopg2.extras.execute_values(self.cur, self.upsert_sql, self._batch)
            self.conn.commit()
            self.log.increment_processed(len(self._batch))
        except Exception as e:
            self.conn.rollback()
            self.log.increment_failed(len(self._batch))
            self.log.event(
                f"batch upsert failed ({len(self._batch)} rows): {e}",
                level="error",
                stage="upsert",
            )
            raise
        finally:
            self._batch.clear()

    def close_spider(self, spider):
        exc_info = (None, None, None)
        try:
            self._flush()
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
        # Spider yields date as "DD/MM/YYYY"
        date_str = item.get("date", "")
        try:
            rate_date = datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            self.log.increment_failed()
            self.log.event(
                f"invalid date '{date_str}' — skipping row",
                level="warning",
                stage="process",
            )
            return item

        # Column names come from the HTML table headers after normalization.
        # BKAM table: Pays, Devise/Devises, Unité, Cours acheteur, Cours vendeur
        currency = (
            str(item.get("devise") or item.get("devises") or "").strip()
        )
        currency = self.shorthand_dict.get(currency, currency)
        if not currency:
            self.log.increment_failed()
            self.log.event("missing currency — skipping row", level="warning", stage="process")
            return item

        country = str(item.get("pays", "") or "").strip() or None
        unit = _parse_unit(item.get("unité") or item.get("unite") or item.get("unites"))
        buy_rate = _parse_rate(item.get("cours_acheteur"))
        sell_rate = _parse_rate(item.get("cours_vendeur"))

        self._batch.append((rate_date, currency, country, unit, buy_rate, sell_rate))
        if len(self._batch) >= BATCH_SIZE:
            self._flush()
        return item
