from io import StringIO
import scrapy
import pandas as pd
from datetime import datetime, timedelta
import urllib.parse


class BkamSpider(scrapy.Spider):
    name = "bkam"
    def __init__(self,start_d =  "01-01-2018" , end =  "31-08-2025" , *args, **kwargs):
        super(BkamSpider, self).__init__(*args, **kwargs)
        self.start_d = start_d
        self.end = end
        self.items_scraped = 0
    def start_requests(self):
        url = "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-des-changes/Cours-de-change/Cours-des-billets-de-banque-etrangers"
        yield scrapy.Request(url=url, callback=self.parse_block)

    def parse_block(self, response):
        try :
            start_date =datetime.strptime(self.start_d, "%d-%m-%Y")
            end_date = datetime.strptime(self.end, "%d-%m-%Y")
        except ValueError as e:
            print("Error:", e)
            return
        #adress = response.css('form::attr(action)').get().split("#")[-1]
        #send_telegram_notification(adress)
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%d/%m/%Y")
            encoded_date = urllib.parse.quote(date_str, safe="")
            url = (
                "https://www.bkam.ma/Marches/Principaux-indicateurs/"
                f"Marche-des-changes/Cours-de-change/Cours-des-billets-de-banque-etrangers"
            )
            yield scrapy.FormRequest.from_response(
                response,
                formcss='form.form-filter',
                formdata={
                    "date": date_str
                },
                method="GET",
                callback=self.parse_data,
                cb_kwargs={"date": date_str}
            )

            current_date += timedelta(days=1)

    def parse_data(self, response, date):
        print(response.url)
        table_html = response.css('table.dynamic_contents_ref_19').get()
        if not table_html:
            self.logger.warning("No table found on the page.")
            
            return
        sio = StringIO(table_html)

        html_str = sio.getvalue()
        html_str = html_str.replace(",", ".")

        sio = StringIO(html_str)
        df = pd.read_html(sio, flavor='html5lib')[0]
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        df['date'] = date
        for row in df.to_dict(orient='records'):
            yield row
        



