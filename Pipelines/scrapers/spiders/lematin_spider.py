import scrapy
from scrapy import Selector
from scrapy_playwright.page import PageMethod


class LematinSpider(scrapy.Spider):
    name = "lematin"
    base_url = "https://lematin.ma/bourse-de-casablanca/liste-societes-cotees?pgno={}"
    total_pages = 6

    def start_requests(self):
        for page in range(1, self.total_pages + 1):
            yield scrapy.Request(
                url=self.base_url.format(page),
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_selector", "div.societeinlist", timeout=15000),
                    ],
                    "page_num": page,
                },
                callback=self.parse,
                errback=self.errback,
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        page_num = response.meta["page_num"]

        # Get content from the live page after JS has fully rendered
        content = await page.content()
        sel = Selector(text=content)
        rows = sel.css("div.societeinlist")

        for row in rows:
            yield {
                "libelle": row.css("div.infos p:nth-child(1) a::text").get("").strip(),
                "secteur": row.css("div.infos p:nth-child(2) a::text").get("").strip(),
                "ticker": (row.css("div.infos p:nth-child(3)::text").getall() or [""])[-1].strip(),
                "siege_social": (row.css("div.infos p:nth-child(4)::text").getall() or [""])[-1].strip(),
                "cours": (row.css("div.infocours div.a li.green::text, div.infocours div.a li.red::text").get() or "").strip(),
                "variation": (row.css("div.infocours div.b li.green::text, div.infocours div.b li.red::text").get() or "").strip(),
            }

        self.logger.info(f"Page {page_num} scraped — {len(rows)} rows found")
        await page.close()

    async def errback(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Request failed: {failure}")
