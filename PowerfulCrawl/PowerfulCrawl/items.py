# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PowerfulCrawlItem(scrapy.Item):
    task_record_id = scrapy.Field()
    task_id = scrapy.Field()
    news_title = scrapy.Field()
    news_author = scrapy.Field()
    news_publish_time = scrapy.Field()
    new_content_text = scrapy.Field()
    news_content_html = scrapy.Field()
    standard_img_url_url = scrapy.Field()
    web_url = scrapy.Field()
    insert_time = scrapy.Field()