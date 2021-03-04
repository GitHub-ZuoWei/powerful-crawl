# -*- coding: utf-8 -*-
import io
import random
import re
import json
import requests

from scrapy import signals
from PIL import Image  # 识别图片文件格式
from PIL import UnidentifiedImageError  # 识别图片格式异常
from minio import Minio  # 图片服务

from lxml import etree
from scrapy import Request, Spider
from requests.adapters import HTTPAdapter
from pyvirtualdisplay import Display  # Linux下无头模式 防检测
from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By  # 导入By模块
from selenium.webdriver.support.wait import WebDriverWait  # 导入等待模块
from selenium.webdriver.support import expected_conditions as EC  # 导入预期条件模块
from selenium.common.exceptions import *  # 异常模块
from scrapy.utils.project import get_project_settings
from selenium.webdriver.common.keys import Keys  # 导入键盘模块
from selenium.webdriver.common.action_chains import ActionChains  # 导入键盘模块
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# 忽略 禁用SSL 警告
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 导入 Mysql
from PowerfulCrawl.utils.mysql_utils import MySQLUtils
# 智能化解析
from readability import Document
from gne import GeneralNewsExtractor

from PowerfulCrawl.utils.format_date_util import powerful_format_date  # 格式化时间
from PowerfulCrawl.utils.common_utils import *  # 通用工具类
from PowerfulCrawl.items import PowerfulCrawlItem

"""
      ┏┛ ┻━━━━━┛ ┻┓
      ┃　　　　　　 ┃
      ┃　　　━　　　┃
      ┃　┳┛　  ┗┳　┃
      ┃　　　　　　 ┃
      ┃　　　┻　　　┃
      ┃　　　　　　 ┃
      ┗━┓　　　┏━━━┛
        ┃　　　┃   神兽保佑
        ┃　　　┃   代码无BUG！
        ┃　　　┗━━━━━━━━━┓
        ┃　　　　　　　    ┣┓
        ┃　　　　         ┏┛
        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
          ┃ ┫ ┫   ┃ ┫ ┫
          ┗━┻━┛   ┗━┻━┛
"""


class PowerfulCrawlSpider(Spider):
    name = 'powerful_crawl'
    allowed_domains = ['powerful_crawl.com']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.settings = get_project_settings()
        # 任务ID
        self.task_id = kwargs.get('task_id')
        # 任务记录表ID
        self.task_record_id = generate_uuid()
        # 记录新闻详情页 插入数据的数量
        self.insert_number = 0
        # 记录新闻详情页 失败的数量
        self.failed_num = 0
        # 初始化Mysql
        self.sql_util = MySQLUtils()
        # 初始化 Minio
        self.minio = Minio(endpoint=self.settings.get("MINIO_URL"), access_key=self.settings.get("MINIO_USER"),
                           secret_key=self.settings.get("MINIO_PWD"), secure=False)
        self.minio_bucket_name = self.settings.get("MINIO_BUCKET")
        # 初始化selenium
        self.option = ChromeOptions()
        # 初始化requests
        self.request = requests.session()
        self.request.adapters.DEFAULT_RETRIES = 3  # 增加requests重连次数
        self.request.mount('https://', HTTPAdapter(max_retries=3))  # 增加requests重连次数
        self.request.mount('http://', HTTPAdapter(max_retries=3))  # 增加requests重连次数
        self.request.keep_alive = False  # 关闭多余连接
        self.request.trust_env = False  # SSL Error
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # 关闭警告信息
        # self.option.add_argument('--disable-extensions')
        # self.option.add_argument('--headless')
        # self.option.add_argument('--disable-gpu')
        self.option.add_argument('--no-sandbox')
        # 去掉提示 Chrome正收到自动测试软件的控制
        self.option.add_argument('disable-infobars')
        # 忽略 ERROR:ssl_client_socket_impl.cc(962) handshake failed; returned -1, SSL error code 1, net_error -100
        self.option.add_argument('--ignore-certificate-errors')
        # self.option.add_argument('--ignore-ssl-errors')
        # 隐身模式启动  但是无法启用插件了.....
        # self.option.add_argument('incognito')
        # 添加代理
        # self.option.add_argument('--proxy-server=http://{ip}:{port}')
        # 跳过webdriver检测  以键值对的形式加入参数
        self.option.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.option.add_experimental_option('useAutomationExtension', False)
        self.option.add_argument("--disable-blink-features=AutomationControlled")

        # 不加载图片
        # self.option.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
        # 禁用Cookies
        self.option.add_experimental_option("prefs", {"profile.default_content_settings.cookies": 2})
        # 加载去广告插件 ADBLOCK_PLUS
        self.option.add_extension(self.settings.get("ADBLOCK_PLUS_PATH"))
        # self.option.add_argument("load-extension=D:\\3.10.2_0")

        # 随机 UA
        self.option.add_argument('user-agent=' + random.choice(self.settings.get('USER_AGENT_LIST')))

        # debug 模式
        # options = Options()
        # options.add_experimental_option('debuggerAddress', '127.0.0.1:9222')

        # 虚拟桌面 Linux
        # self.display = Display(visible=0, size=(1920, 1080))
        # self.display.start()

        # 修改页面加载策略
        # desired_capabilities = DesiredCapabilities.CHROME
        # 注释这两行会导致最后输出结果的延迟，即等待页面加载完成再输出
        # desired_capabilities["pageLoadStrategy"] = "none"

        # self.driver = Chrome(options=self.option, executable_path=self.settings.get('CHROME_DRIVER_PATH'))
        self.driver = Chrome(chrome_options=self.option, executable_path=self.settings.get('CHROME_DRIVER_PATH'))
        # 浏览器窗口大小
        # self.driver.set_window_size(width=1920, height=1080)
        self.driver.maximize_window()
        # 设置模拟浏览器最长等待时间
        self.driver.set_page_load_timeout(60)
        self.driver.set_script_timeout(60)
        # 跳过检测  chrome88以上这个参数不起作用....
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
            """
        })

        # 防止检测浏览器内核访问   也没用 .....
        # script = '''Object.defineProperties(navigator, {webdriver:{get:()=>undefined}})'''
        # self.driver.execute_script(script)
        # 键盘输入
        # ActionChains(self.driver).send_keys(Keys.CONTROL, 'w').perform()
        # ActionChains(self.driver).key_down(Keys.CONTROL).send_keys('w').key_up(Keys.CONTROL).perform()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(PowerfulCrawlSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        # 采集完成 更新任务执行状态
        self.sql_util.update('UPDATE `collect_task_detail` '
                             'SET finish_time="%s",status=%s,num=%s,failed_num=%s '
                             'where id="%s"'
                             % (current_time(), 1, self.insert_number, self.failed_num, self.task_record_id))
        spider.logger.info('Spider will close: %s', spider.name)
        spider.logger.info('Selenium is closing: %s', spider.name)
        if self.driver:
            self.driver.quit()
        # 虚拟桌面 Linux
        # if self.display:
        #     self.display.stop()
        spider.logger.info('Selenium closed: %s', spider.name)
        spider.logger.info('Display closed: %s', spider.name)

    def start_requests(self):
        # 获取当前窗口句柄（窗口A）
        handle = self.driver.current_window_handle
        # 获取当前所有窗口句柄（窗口A、B）
        handles = self.driver.window_handles
        # self.driver.switch_to.window(handles[-1])  # 跳转到最新的句柄
        # 对窗口进行遍历
        for new_handle in handles:
            # 筛选新打开的窗口B
            if new_handle != handle:
                # 切换到新打开的窗口B
                self.driver.switch_to.window(new_handle)
                # 关闭当前窗口B
                self.driver.close()
                # 切换回窗口A
                self.driver.switch_to.window(handles[0])

        # print(self.settings.attributes.keys())
        # 任务开始时插入 collect_task_detail表 任务开始记录
        self.sql_util.insert("INSERT INTO collect_task_detail (id,task_id,start_time,status) "
                             "VALUES ('" + self.task_record_id + "','" + self.task_id + "','" + current_time() + "','0')")

        # 查询任务详情
        task_detail = self.sql_util.fetch_one("select * from collect_task where id = '" + self.task_id + "'")
        # 增量数量
        increment_collect_page_num = int(task_detail['coll_pages'])
        # 全量数量
        first_coll_page_num = int(task_detail['first_coll_pages'])

        # 采集任务类型  增量采集 1 、全量采集 0
        collect_type = task_detail['exec_flag']
        if collect_type == 0:
            collect_page_num = first_coll_page_num
            # 第一次为全量任务  之后改为增量任务采集
            self.sql_util.update('UPDATE `collect_task` SET exec_flag=%s where id="%s"' % (1, self.task_id))
        else:
            collect_page_num = increment_collect_page_num

        # 根据任务ID查询模板规则
        template_detail = self.sql_util.fetch_one("SELECT * FROM `collect_template` WHERE id = '"
                                                  + task_detail['template_id'] + "'")
        # 根据任务ID 查询新闻详情页规则
        self.news_detail_rule = self.sql_util.fetch_one(
            "SELECT * FROM `collect_template` WHERE type = 'detail' AND board_id = " \
            "( SELECT board_id FROM `collect_template` WHERE id = " \
            "( SELECT template_id FROM collect_task WHERE id = '" + self.task_id + "' ) )")
        if self.news_detail_rule:
            self.news_detail_rule = json.loads(self.news_detail_rule['content'])[0]

        # 列表页 URL
        news_url = template_detail['address']
        # 域名
        self.domain_url = re.match(r'(\w+)://([^/:]+)(:\d*)?', news_url).group()
        # 新闻列表规则
        news_list_rule = json.loads(template_detail['content'])
        # 请求列表页
        try:
            self.driver.get(news_url)
        except TimeoutException:
            self.sql_util.update(
                'UPDATE `collect_task_detail` SET error="%s",status = "%s" where id="%s"' % (
                    '2', '2', self.task_record_id))

        # 单列表
        if len(news_list_rule) == 1:
            for news_list_rule_item in news_list_rule:
                # 翻页方式 ['':'无翻页']、[auto:'滚动瀑布流']、[manual:'手动瀑布流]、['pagination:'页码翻页']
                news_list_load_type = news_list_rule_item[
                    'listLoadType'] if 'listLoadType' in news_list_rule_item else None
                # 翻页按钮
                news_list_next_button = news_list_rule_item[
                    'listNextBtn'] if 'listNextBtn' in news_list_rule_item else None
                # 列表容器
                news_list_container = news_list_rule_item[
                    'listContainer'] if 'listContainer' in news_list_rule_item else None

                # 列表页面解析
                if news_list_load_type == 'manual':
                    print('manual')
                    # 调用滚动点击方法
                    self.scroll_click(click_num=collect_page_num, click_button_xpath=news_list_next_button,
                                      load_type=news_list_load_type)
                    yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url,
                                                  news_list_container)
                if news_list_load_type == 'pagination':
                    # 下一页按钮
                    print('pagination')
                    yield from self.analysis_click_next_list(collect_page_num=collect_page_num,
                                                             news_list_next_button=news_list_next_button,
                                                             news_list_rule=news_list_rule_item['listFirstData'])
                if news_list_load_type == 'auto':
                    print('auto')
                    # 瀑布流
                    self.scroll_click(click_num=collect_page_num, load_type=news_list_load_type)
                    yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url,
                                                  news_list_container)
                if news_list_load_type == 'auto-manual':
                    print('auto-manual')
                    # 自动瀑布流加手动瀑布流
                    self.scroll_click(click_num=collect_page_num, click_button_xpath=news_list_next_button,
                                      load_type=news_list_load_type)
                    yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url,
                                                  news_list_container)
                if not news_list_load_type:
                    print('none')
                    # 无需翻页
                    yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url,
                                                  news_list_container)
        # 多列表
        else:
            # 无需翻页的多列表新闻URL
            news_list_url = []
            for news_list_rule_item in news_list_rule:
                # 翻页方式 ['':'无翻页']、[auto:'滚动瀑布流']、[manual:'手动瀑布流]、['pagination:'页码翻页']
                news_list_load_type = news_list_rule_item[
                    'listLoadType'] if 'listLoadType' in news_list_rule_item else None
                # 翻页按钮
                news_list_next_button = news_list_rule_item[
                    'listNextBtn'] if 'listNextBtn' in news_list_rule_item else None
                # 列表容器
                news_list_container = news_list_rule_item[
                    'listContainer'] if 'listContainer' in news_list_rule_item else None

                if not news_list_load_type or not news_list_next_button:
                    # 抽取详情页URL
                    extractor = ListPageExtractor()
                    result = extractor.extract(html=self.driver.page_source,
                                               feature=news_list_rule_item['listFirstData'],
                                               domain=self.domain_url)
                    for item in result:
                        news_list_url.append(item['url'])

                else:
                    # 列表页面解析
                    if news_list_load_type == 'manual':
                        print('manual')
                        # 调用滚动点击方法
                        self.scroll_click(click_num=collect_page_num, click_button_xpath=news_list_next_button,
                                          load_type=news_list_load_type)
                        yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url,
                                                      news_list_container)
                    if news_list_load_type == 'pagination':
                        # 下一页按钮
                        print('pagination')
                        yield from self.analysis_click_next_more_list(collect_page_num=collect_page_num,
                                                                      news_list_next_button=news_list_next_button,
                                                                      news_list_rule=news_list_rule_item[
                                                                          'listFirstData'],
                                                                      all_news_list_rule=news_list_rule)
                    if news_list_load_type == 'auto':
                        print('auto')
                        # 瀑布流
                        self.scroll_click(click_num=collect_page_num, load_type=news_list_load_type)
                        yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url,
                                                      news_list_container)
                    if news_list_load_type == 'auto-manual':
                        print('auto-manual')
                        # 自动瀑布流加手动瀑布流
                        self.scroll_click(click_num=collect_page_num, click_button_xpath=news_list_next_button,
                                          load_type=news_list_load_type)
                        yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url,
                                                      news_list_container)

            self.logger.info('正在将无需翻页的多列表新闻种子加入布隆过滤器....')
            for news_url in news_list_url:
                yield Request(url=news_url, callback=self.parse)

    def parse(self, response):
        # 解析详情页
        try:
            self.driver.get(response.url)
        except TimeoutException:
            self.failed_num += 1
        # 用户自定义规则
        # news_detail_title_rule = self.news_detail_rule['title']
        # news_detail_author_rule = self.news_detail_rule['author']
        # news_detail_pubTime_rule = self.news_detail_rule['pubTime']
        # news_detail_content_rule = self.news_detail_rule['content']
        # news_detail_source_rule = self.news_detail_rule['source']

        # 滚动浏览器至中间位置 PS:解决新闻图片懒加载问题
        scroll_to_centre(self.driver)
        time.sleep(1)
        # 滚动浏览器至底部 PS:解决新闻图片懒加载问题
        scroll_to_bottom(self.driver)
        time.sleep(1)

        # 获取加载完成的页面源代码
        origin_code = self.driver.page_source
        document = Document(origin_code)
        extractor = GeneralNewsExtractor()
        # 使用用户指定规则
        # extract_result = extractor.extract(origin_code, title_xpath=news_detail_title_rule,
        #                                    author_xpath=news_detail_author_rule,
        #                                    publish_time_xpath=news_detail_pubTime_rule, host=self.domain_url,
        #                                    body_xpath=news_detail_content_rule)

        extract_result = extractor.extract(html=origin_code, with_body_html=True)
        # 新闻作者
        news_author = extract_result['author']
        # 新闻发布时间   先根据规则自动抽取
        news_publish_time = extract_result['publish_time']
        # 新闻标题
        news_title_gne = extract_result['title']
        # 新闻内容
        news_content_gne = extract_result['content']
        # 新闻内容 HTML
        news_content_html_gne = extract_result['body_html']

        if news_publish_time:
            # 2021-01-21 22:47:00+08:00
            news_publish_time = powerful_format_date(news_publish_time).split('+')[0][:19]
        else:
            # 没有匹配到  使用用户自定义的规则
            try:
                # news_publish_time = powerful_format_date(self.driver.find_element_by_xpath(news_detail_pubTime_rule).text)
                news_publish_time = current_time()
            except NoSuchElementException:
                # 还没有匹配到  使用系统当前时间  ^_^ !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                news_publish_time = current_time()

        # 新闻标题
        # news_title = extract_result['title']
        # 新闻标题
        news_title = document.short_title()
        # 新闻详情页HTML
        news_content_html = document.summary(html_partial=True)
        html = etree.HTML(news_content_html)
        # 新闻详情页文本
        new_content_text = html.xpath('string(.)').strip()

        # 过滤 新闻标题或者新闻内容没有数据
        if not news_title or not news_content_html or not new_content_text.strip():
            # 解析失败数量加1
            self.failed_num += 1
            return

        # 下载新闻图片
        news_content_html, remote_img_url, local_img_url = self.download_news_img(html=html, response=response,
                                                                                  news_content_html=news_content_html)

        # 判断 如果Document识别纯文字长度小于GNE文字长度  并且 没有下载过图片
        # if len(new_content_text) + 200 < len(news_content_gne) and not remote_img_url:
        #     new_content_text = news_content_gne
        #     news_content_html = news_content_html_gne

        news_item = PowerfulCrawlItem()
        news_item['task_id'] = self.task_id
        news_item['title'] = news_title
        news_item['author'] = news_author
        news_item['pub_time'] = news_publish_time
        news_item['content'] = new_content_text
        news_item['content_html'] = news_content_html
        news_item['remote_img_url'] = remote_img_url
        news_item['local_img_url'] = local_img_url
        news_item['url'] = response.url
        news_item['create_time'] = current_time()
        self.insert_number += 1
        yield news_item

    def analysis_list(self, news_list_rule, domain_url, news_list_container):
        """
        @summary: 解析滚动点击/瀑布流列表/列表先瀑布流后滚动点击
        @param news_list_rule:列表规则
        @param domain_url: 网站域名domain
        @return: 回调self.parse 方法
        """
        # 获取加载之后的源代码
        origin_code = self.driver.page_source
        extractor = ListPageExtractor()
        result = extractor.extract(html=origin_code,
                                   feature=news_list_rule,
                                   domain=domain_url)
        # 如果没有提取到列表,莫得办法了^_^
        if not result:
            self.logger.info('未提取到新闻URL,尝试使用系统预设规则匹配')
            extract_list_html = etree.HTML(origin_code)
            news_list_container_xpath = extract_list_html.xpath(news_list_container)
            if news_list_container_xpath:
                news_url_list = []
                for news_element in news_list_container_xpath[0].xpath('*'):
                    href = ''.join(news_element.xpath('.//a/@href'))
                    if href:
                        if href.startswith('http'):
                            news_url_list.append(href)
                        elif href.startswith('//'):
                            news_url_list.append(domain_url.split('//')[0] + href)
                        elif href.startswith('/'):
                            news_url_list.append(domain_url + href)
                        elif href.startswith('../'):
                            news_url_list.append(domain_url + href[2:])
                        else:
                            news_url_list.append(domain_url + '/' + href)

                self.logger.info('使用系统预设规则匹配完成,共提取' + str(len(set(news_url_list))) + '个新闻详情页URL')
                self.logger.info('正在将新闻种子加入布隆过滤器....')
                for news_url in set(news_url_list):
                    yield Request(url=news_url, callback=self.parse)
                return

            # 列表模板配置错误或失效
            self.sql_util.update(
                'UPDATE `collect_task_detail` SET error="%s",status = "%s" where id="%s"' % (
                    '1', '2', self.task_record_id))

        self.logger.info('提取新闻列表页完成,共提取' + str(len(result)) + '个新闻详情页URL')
        self.logger.info('正在将新闻种子加入布隆过滤器....')
        for news_url in result:
            yield Request(url=news_url['url'], callback=self.parse)

    def analysis_click_next_list(self, collect_page_num, news_list_rule, news_list_next_button):
        """
        @summary: 解析点击下一页按钮 列表
        @param news_list_rule:列表规则
        @param domain_url: 网站域名domain
        @return: 回调self.parse 方法
        """
        all_news_detail_url = []
        # 点击下一页按钮
        for click_num in range(collect_page_num):
            # 解析列表第一页
            if click_num == 0:
                time.sleep(random.uniform(1, 3))
                result = list_page_extractor(self.driver.page_source, news_list_rule, self.domain_url)
                for news_url in result:
                    # print(news_url['url'])
                    all_news_detail_url.append(news_url['url'])
                continue
            # 解析列表其他项
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.XPATH, news_list_next_button)))
            except TimeoutException:
                continue
            click_button_by_xpath(self.driver, news_list_next_button)
            time.sleep(random.uniform(2, 3))
            result = list_page_extractor(self.driver.page_source, news_list_rule, self.domain_url)
            for news_url in result:
                # print(news_url['url'])
                all_news_detail_url.append(news_url['url'])
        self.logger.info('提取新闻列表页完成,共提取' + str(len(all_news_detail_url)) + '个URL')
        self.logger.info('提取新闻列表页完成,去重后共提取' + str(len(set(all_news_detail_url))) + '个URL')
        self.logger.info('正在将新闻种子加入布隆过滤器....')
        for news_detail_url in set(all_news_detail_url):
            # print(news_detail_url)
            yield Request(url=news_detail_url, meta={
                'dont_redirect': True,
                'handle_httpstatus_list': [302]
            }, callback=self.parse)

    def analysis_click_next_more_list(self, collect_page_num, news_list_rule, news_list_next_button,
                                      all_news_list_rule):
        """
        @summary: 解析点击下一页按钮 多列表
        @param news_list_rule:列表规则
        @param domain_url: 网站域名domain
        @return: 回调self.parse 方法
        """
        all_news_detail_url = []
        # 点击下一页按钮
        for click_num in range(collect_page_num):
            # 解析列表第一页
            if click_num == 0:
                time.sleep(random.uniform(1, 3))
                result = list_page_extractor(self.driver.page_source, news_list_rule, self.domain_url)
                for news_url in result:
                    all_news_detail_url.append(news_url['url'])
                continue
            # 解析列表其他项
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.XPATH, news_list_next_button)))
            except TimeoutException:
                continue
            click_button_by_xpath(self.driver, news_list_next_button)
            time.sleep(random.uniform(2, 3))

            # 抽取第二页里的所有列表
            for news_list_rule_item in all_news_list_rule:
                result = list_page_extractor(self.driver.page_source, news_list_rule_item['listFirstData'],
                                             self.domain_url)
                for news_url in result:
                    # print(news_url['url'])
                    all_news_detail_url.append(news_url['url'])

        self.logger.info('提取新闻列表页完成,共提取' + str(len(all_news_detail_url)) + '个URL')
        self.logger.info('提取新闻列表页完成,去重后共提取' + str(len(set(all_news_detail_url))) + '个URL')
        self.logger.info('正在将新闻种子加入布隆过滤器....')
        for news_detail_url in set(all_news_detail_url):
            # print(news_detail_url)
            yield Request(url=news_detail_url, meta={
                'dont_redirect': True,
                'handle_httpstatus_list': [302]
            }, callback=self.parse)

    def scroll_click(self, click_num, click_button_xpath=None, load_type=None):
        """
        @summary: 列表滚动点击 / 列表瀑布流 / 列表先瀑布流后滚动点击
        @param click_num: 点击/滚动次数
        @param click_button_xpath: 点击按钮Xpath规则
        @param load_type: 列表加载方式
        @return: None
        """
        js = "return action=document.body.scrollHeight"
        time.sleep(random.uniform(1, 3))
        height = self.driver.execute_script(js)
        # 将滚动条调整至页面底部
        scroll_to_bottom(self.driver)
        # 定义初始时间戳（秒）
        t1 = int(time.time())
        # 翻页次数
        page_num = 0
        retry_num = 0
        while True:
            if page_num >= click_num:
                self.logger.info("当前页: " + str(page_num))
                return
            # 获取当前时间戳（秒）
            t2 = int(time.time())
            # 判断时间初始时间戳和当前时间戳相差是否大于6秒，小于6秒则下拉滚动条
            if t2 - t1 < 6:
                new_height = self.driver.execute_script(js)

                # 手动瀑布流
                if load_type == 'manual':
                    # 滚动至底部
                    scroll_to_bottom(self.driver)
                    # 等待加载更多按钮
                    try:
                        wait = WebDriverWait(self.driver, 5)
                        wait.until(EC.presence_of_element_located((By.XPATH, click_button_xpath)))
                    except TimeoutException:
                        continue
                    try:
                        self.logger.info('手动瀑布流点击' + str(page_num + 1) + '次')
                        click_button_by_xpath(self.driver, click_button_xpath)
                        self.logger.info('手动瀑布流点击完成,随机等待2~3秒,等待页面加载.....')
                        time.sleep(random.uniform(2, 3))
                    except ElementNotInteractableException:
                        self.logger.warning('不能点 .                   可恶啊')
                        # 列表模板配置错误或失效
                        self.sql_util.update(
                            'UPDATE `collect_task_detail` SET error="%s",status = "%s" where id="%s"' % (
                                '1', '2', self.task_record_id))

                # 先自动瀑布流后点击瀑布流
                if load_type == 'auto-manual':
                    # 滚动到底部3次
                    for scroll in range(3):
                        time.sleep(1)
                        scroll_to_bottom(self.driver)
                    # 等待加载更多按钮
                    try:
                        wait = WebDriverWait(self.driver, 5)
                        wait.until(EC.presence_of_element_located((By.XPATH, click_button_xpath)))
                    except TimeoutException:
                        continue
                    try:
                        self.logger.info('自动瀑布流后点击瀑布流,点击' + str(page_num + 1) + '次')
                        click_button_by_xpath(self.driver, click_button_xpath)
                        self.logger.info('自动瀑布流后点击瀑布流点击完成,随机等待2~3秒,等待页面加载.....')
                        time.sleep(random.uniform(2, 3))
                    except ElementNotInteractableException:
                        self.logger.warning('不能点 .                   可恶啊')
                        # 列表模板配置错误或失效
                        self.sql_util.update(
                            'UPDATE `collect_task_detail` SET error="%s",status = "%s" where id="%s"' % (
                                '1', '2', self.task_record_id))

                if new_height > height:
                    page_num += 1
                    # 滚动至底部
                    scroll_to_bottom(self.driver)
                    time.sleep(random.uniform(2, 3))
                    # 重置初始页面高度
                    height = new_height
                    # 重置初始时间戳，重新计时
                    t1 = int(time.time())
            elif retry_num < 3:  # 当超过6秒页面高度仍然没有更新时，进入重试逻辑，重试3次，每次等待3 秒
                time.sleep(3)
                retry_num = retry_num + 1
            else:  # 超时并超过重试次数，程序结束跳出循环，并认为页面已经加载完毕！
                self.logger.info("滚动条已经处于页面最下方！")
                # 滚动条调整至页面顶部
                scroll_to_top(self.driver)
                return

    def download_news_img(self, html, response, news_content_html):
        """
        @summary: 下载新闻详情页图片
        @param html: 新闻内容 etree.HTML 元素
        @param response: response.url  新闻详情页URL
        @param news_content_html: 新闻内容 文本HTML 元素
        @return: news_content_html(处理完图片src='***'之后的文本HTML) 、standard_img_url_url(图片列表集合)
        """
        # 本篇新闻的图片链接
        remote_img_url_list = []
        # 本篇新闻的图片处理之后的本地文件名称
        local_img_url_list = []
        # 下载图片
        for img_url in html.xpath('//img/@src'):
            if not img_url:
                continue
            if img_url.startswith('//'):
                standard_img_url = response.url.split('//')[0] + img_url
            elif img_url.startswith('/'):
                standard_img_url = self.domain_url + img_url
            elif img_url.startswith('http'):
                standard_img_url = img_url
            else:
                # src="1127063095_16124076686341n.jpg"
                standard_img_url = '/'.join(response.url.split('/')[:-1]) + '/' + img_url

            img_response = self.request.get(standard_img_url, timeout=20,
                                            headers={'user-agent': random.choice(self.settings.get('USER_AGENT_LIST'))},
                                            verify=False)
            if img_response.status_code == 200:
                # 将图片内容转换为 Bytes
                bytes_img_content = io.BytesIO(img_response.content)
                # 过滤小于1024 Bytes的图片
                if len(bytes_img_content.getvalue()) > 1024:
                    # 获取图片文件格式
                    try:
                        img_postfix = Image.open(bytes_img_content).format.lower()
                    except UnidentifiedImageError:
                        self.logger.warn('识别图片格式异常 url:' + standard_img_url)
                        continue
                    # 图片文件名称
                    img_full_name = 'newsimg_' + time.strftime("%Y_%m_%d_") + \
                                    str(int(round(time.time() * 1000))) + '.' + img_postfix
                    # print(standard_img_url)
                    remote_img_url_list.append(standard_img_url)
                    local_img_url_list.append(img_full_name)
                    self.minio.put_object(bucket_name=self.minio_bucket_name, object_name=img_full_name,
                                          data=io.BytesIO(img_response.content),
                                          length=-1, content_type='image/png', part_size=10 * 1024 * 1024)
                    # 替换HTML中的图片URL地址为  桶名 + 文件名
                    news_content_html = news_content_html.replace(img_url.replace('&', '&amp;'),
                                                                  "/" + self.minio_bucket_name + "/" + img_full_name)
            else:
                print(standard_img_url)
                print(img_response.status_code)
        return news_content_html, remote_img_url_list, local_img_url_list
