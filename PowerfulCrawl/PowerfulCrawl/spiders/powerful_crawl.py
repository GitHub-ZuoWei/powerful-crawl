# -*- coding: utf-8 -*-
import scrapy


class PowerfulCrawlSpider(scrapy.Spider):
    name = 'powerful_crawl'
    allowed_domains = ['crawl.com']
    start_urls = ['http://crawl.com/']

    def parse(self, response):
        pass
