# -*- coding: utf-8 -*-
from scrapy import Request, Spider


class PowerfulCrawlSpider(Spider):
    name = 'powerful_crawl'
    allowed_domains = ['crawl.com']
    base_url = 'https://www.baidu.com/s?wd='

    def start_requests(self):
        for i in range(10):
            url = self.base_url + str(i)
            yield Request(url, callback=self.parse)

        # Here contains 10 duplicated Requests
        for i in range(100):
            url = self.base_url + str(i)
            yield Request(url, callback=self.parse)

    def parse(self, response):
        pass
