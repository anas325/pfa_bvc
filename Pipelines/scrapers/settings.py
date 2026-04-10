BOT_NAME = "scrapers"

SPIDER_MODULES = ["scrapers.spiders"]
NEWSPIDER_MODULE = "scrapers.spiders"

ROBOTSTXT_OBEY = False

DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {"headless": True}

# Be polite
DOWNLOAD_DELAY = 0.2
CONCURRENT_REQUESTS = 1

FEEDS = {
    "data/%(name)s_%(time)s.json": {
        "format": "json",
        "encoding": "utf-8",
        "indent": 2,
        "overwrite": True,
    }
}
