# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PowerfulCrawlItem(scrapy.Item):
    task_record_id = scrapy.Field()
    task_id = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    pub_time = scrapy.Field()
    content = scrapy.Field()
    content_html = scrapy.Field()
    remote_img_url = scrapy.Field()
    local_img_url = scrapy.Field()
    url = scrapy.Field()
    create_time = scrapy.Field()