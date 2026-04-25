from scrapers.settings import *  # noqa: F401, F403

ITEM_PIPELINES = {
    "scrapers.bkam_pg_pipeline.BkamPostgresPipeline": 1,
}

FEEDS = {}
