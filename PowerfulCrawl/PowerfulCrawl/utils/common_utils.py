import time
import uuid

from gne import ListPageExtractor


# 生成UUID
def generate_uuid():
    return str(uuid.uuid1()).replace('-', '')


# 当前系统时间
def current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S')


# 列表页抽取
def list_page_extractor(origin_code, news_list_rule, domain_url):
    return ListPageExtractor().extract(html=origin_code, feature=news_list_rule, domain=domain_url)


# 通过执行前端JS 点击
def click_button_by_xpath(chrome_driver, click_xpath):
    return chrome_driver.execute_script("document.evaluate('" + click_xpath + "', document).iterateNext().click()")
