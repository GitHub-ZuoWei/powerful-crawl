import time
import uuid

from gne import ListPageExtractor


def generate_uuid():
    return str(uuid.uuid1()).replace('-', '')


def current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S')


def list_page_extractor(origin_code, news_list_rule, domain_url):
    return ListPageExtractor().extract(html=origin_code, feature=news_list_rule, domain=domain_url)
