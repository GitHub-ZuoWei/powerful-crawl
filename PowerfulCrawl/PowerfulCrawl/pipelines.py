# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

from PowerfulCrawl.spiders.powerful_crawl import PowerfulCrawlSpider
from PowerfulCrawl.utils.mysql_utils import MySQLUtils
from PowerfulCrawl.utils.common_utils import current_time


class PowerfulCrawlPipeline(object):

    def __init__(self, mongo_url, mongo_db, mongo_table, mongo_port, mongo_user, mongo_pwd):
        self.mongo_url = mongo_url
        self.mongo_db = mongo_db
        self.mongo_table = mongo_table
        self.mongo_port = mongo_port
        self.mongo_user = mongo_user
        self.mongo_pwd = mongo_pwd
        self.sql_util = MySQLUtils()

    @classmethod
    def from_crawler(cls, crawl):
        return cls(
            mongo_url=crawl.settings.get('MONGO_URL'),
            mongo_db=crawl.settings.get('MONGO_DB'),
            mongo_table=crawl.settings.get('MONGO_TABLE'),
            mongo_port=crawl.settings.get('MONGO_PORT'),
            mongo_user=crawl.settings.get('MONGO_USER'),
            mongo_pwd=crawl.settings.get('MONGO_PWD')
        )

    def open_spider(self, spider):
        """
            爬虫一旦开启，就会实现这个方法，连接到数据库
        """
        self.client = pymongo.MongoClient(self.mongo_url)
        self.db = self.client[self.mongo_db]
        # self.db.authenticate(self.mongo_user, self.mongo_pwd, mechanism='SCRAM-SHA-1')
        self.mongo_client = self.db[self.mongo_table]

    def close_spider(self, spider):
        """
        爬虫一旦关闭，就会实现这个方法，关闭数据库连接
        """
        # print(spider.crawler.stats.get_stats())
        # scrapy_crawl_stats = spider.crawler.stats.get_stats()
        # self.sql_util.update('UPDATE `collect_task_detail` SET finish_time="%s",num=%s,status=%s where id="%s"' % (
        #     current_time(), self.insert_number, 1, self.task_record_id))
        # print(self.task_record_id)
        self.client.close()

    def process_item(self, item, spider):
        if isinstance(spider, PowerfulCrawlSpider):
            self.mongo_client.insert_one(dict(item))
        else:
            return item
