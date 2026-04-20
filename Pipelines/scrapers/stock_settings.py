from scrapers.settings import *  # noqa: F401, F403

ITEM_PIPELINES = {
    "scrapers.pg_pipeline.PostgresStockPipeline": 1,
}
