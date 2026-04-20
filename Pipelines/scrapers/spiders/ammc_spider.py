import scrapy


class AmmcSpider(scrapy.Spider):
    name = "ammc"
    base_url = "https://www.ammc.ma/fr/publications"

    def start_requests(self):
        yield scrapy.Request(self._page_url(0), callback=self.parse, cb_kwargs={"page": 0})

    def _page_url(self, page: int) -> str:
        return f"{self.base_url}?page={page}"

    def parse(self, response, page: int):
        rows = response.css("li.actualites-row")
        if not rows:
            return

        for row in rows:
            title = row.css(".views-field-title .field-content::text").get("").strip()
            file_href = row.css(".views-field-field-attachement a::attr(href)").get()
            if not file_href:
                continue

            date = row.css(".views-field-field-date .field-content::text").get("").strip()
            category = row.css(".views-field-field-type-publication .field-content::text").get("").strip()

            yield {
                "title": title,
                "date": date,
                "category": category,
                "file_url": response.urljoin(file_href),
            }

        yield scrapy.Request(self._page_url(page + 1), callback=self.parse, cb_kwargs={"page": page + 1})
