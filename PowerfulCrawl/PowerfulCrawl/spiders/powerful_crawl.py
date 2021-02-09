# -*- coding: utf-8 -*-
import io
import random
import re
import json
import requests

from scrapy import signals

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
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # 关闭警告信息

        # self.option.add_argument('--disable-extensions')
        # self.option.add_argument('--headless')
        # self.option.add_argument('--disable-gpu')
        # self.option.add_argument('--no-sandbox')
        # 去掉提示 Chrome正收到自动测试软件的控制
        self.option.add_argument('disable-infobars')
        # 隐身模式启动
        # self.option.add_argument('incognito')
        # 添加代理
        # self.option.add_argument('--proxy-server=http://{ip}:{port}')
        # 跳过webdriver检测  以键值对的形式加入参数
        self.option.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.option.add_experimental_option('useAutomationExtension', False)
        self.option.add_argument("--disable-blink-features=AutomationControlled")

        # 不加载图片,加快访问速度
        # self.option.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
        # 加载去广告插件 ADBLOCK_PLUS
        self.option.add_extension(self.settings.get("ADBLOCK_PLUS_PATH"))
        # self.option.add_extension('C:\\Users\hp\Desktop\chrome-plugin-xpath.crx')

        # 随机 UA
        self.option.add_argument('user-agent=' + random.choice(self.settings.get('USER_AGENT_LIST')))

        # debug 模式
        # options = Options()
        # options.add_experimental_option('debuggerAddress', '127.0.0.1:9222')

        # 虚拟桌面 Linux
        # self.display = Display(visible=0, size=(800, 800))
        # self.display.start()

        # self.driver = Chrome(options=self.option, executable_path=self.settings.get('CHROME_DRIVER_PATH'))
        self.driver = Chrome(chrome_options=self.option, executable_path=self.settings.get('CHROME_DRIVER_PATH'))
        # 浏览器窗口大小
        # self.driver.set_window_size(width=1920, height=1080)
        # self.driver.maximize_window()
        # 设置模拟浏览器最长等待时间
        self.driver.set_page_load_timeout(36)
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
        # windows = self.driver.window_handles  # 获取该会话所有的句柄
        # self.driver.switch_to.window(windows[-1])  # 跳转到最新的句柄

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(PowerfulCrawlSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
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
        # print(self.settings.attributes.keys())
        # 任务开始时插入 collect_task_detail表 任务开始记录
        self.sql_util.insert("INSERT INTO collect_task_detail (id,task_id,start_time,status) "
                             "VALUES ('" + self.task_record_id + "','" + self.task_id + "','" + current_time() + "','0')")

        # 查询任务详情
        task_detail = self.sql_util.fetch_one("select * from collect_task where id = '" + self.task_id + "'")
        # 本任务采集页数
        collect_page_num = int(task_detail['coll_pages'])
        # 采集任务类型  增量采集 incrm 、全量采集 all
        collect_type = task_detail['type']

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
        news_list_url = template_detail['address']
        # 域名
        self.domain_url = re.match(r'(\w+)://([^/:]+)(:\d*)?', news_list_url).group()
        # 新闻列表规则
        news_list_rule = json.loads(template_detail['content'])
        # 请求列表页
        self.driver.get(news_list_url)

        # 对配置的列表规则排序  有下一页按钮的放到最后
        # sort_news_list_rule_item = []
        # sort_temp_news_list_rule_item = []
        # all_temp_news_list = []
        # for temp_news_list_rule_item in news_list_rule:
        #     temp_news_list_next_button = temp_news_list_rule_item['listNextBtn']
        #     if not temp_news_list_next_button:
        #         sort_news_list_rule_item.append(temp_news_list_rule_item)
        #         all_temp_news_list.append(temp_news_list_rule_item['listFirstData'])
        #     else:
        #         sort_temp_news_list_rule_item.append(temp_news_list_rule_item)
        #         all_temp_news_list.append(temp_news_list_rule_item['listFirstData'])
        # news_list_rule = sort_news_list_rule_item + sort_temp_news_list_rule_item

        for news_list_rule_item in news_list_rule:
            # 翻页方式 ['':'无翻页']、[auto:'滚动瀑布流']、[manual:'手动瀑布流]、['pagination:'页码翻页']
            news_list_load_type = news_list_rule_item['listLoadType']
            # 翻页按钮
            news_list_next_button = news_list_rule_item['listNextBtn']
            # 列表页面解析
            if news_list_load_type == 'manual':
                print('manual')
                # 调用滚动点击方法
                self.scroll_click(click_num=collect_page_num, click_button_xpath=news_list_next_button,
                                  load_type=news_list_load_type)
                yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url)
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
                yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url)
            if not news_list_load_type:
                print('none')
                # 无需翻页
                yield from self.analysis_list(news_list_rule_item['listFirstData'], self.domain_url)

    def parse(self, response):
        # 解析详情页
        self.driver.get(response.url)
        # 用户自定义规则
        # news_detail_title_rule = self.news_detail_rule['title']
        # news_detail_author_rule = self.news_detail_rule['author']
        # news_detail_pubTime_rule = self.news_detail_rule['pubTime']
        # news_detail_content_rule = self.news_detail_rule['content']
        # news_detail_source_rule = self.news_detail_rule['source']

        self.driver.execute_script('window.scrollTo({top: document.body.scrollHeight,behavior: "smooth"})')
        time.sleep(2)

        # 获取加载完成的页面源代码
        origin_code = self.driver.page_source
        document = Document(origin_code)
        extractor = GeneralNewsExtractor()
        # 使用用户指定规则
        # extract_result = extractor.extract(origin_code, title_xpath=news_detail_title_rule,
        #                                    author_xpath=news_detail_author_rule,
        #                                    publish_time_xpath=news_detail_pubTime_rule, host=self.domain_url,
        #                                    body_xpath=news_detail_content_rule)

        extract_result = extractor.extract(origin_code)
        # 新闻作者
        news_author = extract_result['author']
        # 新闻发布时间   先根据规则自动抽取
        news_publish_time = extract_result['publish_time']
        if news_publish_time:
            # 2021-01-21 22:47:00+08:00
            news_publish_time = powerful_format_date(news_publish_time).split('+')[0][:19]
            if not news_publish_time:
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
        # 下载新闻图片
        news_content_html, standard_img_url_url = self.download_news_img(html=html, response=response,
                                                                         news_content_html=news_content_html)

        news_item = PowerfulCrawlItem()
        news_item['task_record_id'] = self.task_record_id
        news_item['task_id'] = self.task_id
        news_item['news_title'] = news_title
        news_item['news_author'] = news_author
        news_item['news_publish_time'] = str(news_publish_time) if str(news_publish_time) else None
        news_item['new_content_text'] = new_content_text
        news_item['news_content_html'] = news_content_html
        news_item['standard_img_url_url'] = standard_img_url_url
        news_item['web_url'] = response.url
        news_item['insert_time'] = current_time()
        yield news_item

    def analysis_list(self, news_list_rule, domain_url):
        """
        @summary: 解析滚动点击/瀑布流列表
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
        print(len(result))
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
                result = list_page_extractor(self.driver.page_source, news_list_rule, self.domain_url)
                for news_url in result:
                    print(news_url['url'])
                    all_news_detail_url.append(news_url['url'])
                continue
            # 解析列表其他项
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.XPATH, news_list_next_button)))
                self.driver.find_element_by_xpath(news_list_next_button).click()
            except:
                continue

            result = list_page_extractor(self.driver.page_source, news_list_rule, self.domain_url)
            for news_url in result:
                print(news_url['url'])
                all_news_detail_url.append(news_url['url'])

        for news_detail_url in all_news_detail_url:
            yield Request(url=news_detail_url, callback=self.parse)

    def scroll_click(self, click_num, click_button_xpath=None, load_type=None):
        """
        @summary: 列表滚动点击 / 列表瀑布流
        @param click_num: 点击/滚动次数
        @param click_button_xpath: 点击按钮Xpath规则
        @param load_type: 列表加载方式
        @return: None
        """
        js = "return action=document.body.scrollHeight"
        height = self.driver.execute_script(js)
        # 将滚动条调整至页面底部
        self.driver.execute_script('window.scrollTo({top: document.body.scrollHeight,behavior: "smooth"})')
        # 定义初始时间戳（秒）
        t1 = int(time.time())
        # 重试次数
        retry_num = 0
        # 翻页次数
        page_num = 0
        while True:
            if page_num >= click_num:
                print("pagenum: " + str(page_num))
                return True
            # 获取当前时间戳（秒）
            t2 = int(time.time())
            # 判断时间初始时间戳和当前时间戳相差是否大于6秒，小于6秒则下拉滚动条
            if t2 - t1 < 6:
                new_height = self.driver.execute_script(js)
                if load_type == 'manual':
                    scroll_retry_times = 0
                    self.driver.execute_script('window.scrollTo({top: document.body.scrollHeight,behavior: "smooth"})')
                    # 等待加载更多按钮
                    wait = WebDriverWait(self.driver, 5)
                    wait.until(EC.presence_of_element_located((By.XPATH, click_button_xpath)))
                    try:
                        self.driver.find_element_by_xpath(click_button_xpath).click()
                    except ElementClickInterceptedException:
                        retry_times = 3  # 重试的次数
                        while scroll_retry_times < retry_times:
                            scroll_retry_times += 1
                            # 将滚动条调整至页面底部
                            self.driver.execute_script(
                                'window.scrollTo({top: document.body.scrollHeight,behavior: "smooth"})')
                            new_height = self.driver.execute_script(js)
                    except ElementNotInteractableException:
                        print('不能点 .                   可恶啊')
                if new_height > height:
                    page_num += 1
                    time.sleep(1)
                    self.driver.execute_script('window.scrollTo({top: document.body.scrollHeight,behavior: "smooth"})')
                    time.sleep(1)
                    # 重置初始页面高度
                    height = new_height
                    # 重置初始时间戳，重新计时
                    t1 = int(time.time())
            elif retry_num < 3:  # 当超过6秒页面高度仍然没有更新时，进入重试逻辑，重试3次，每次等待3 秒
                time.sleep(3)
                retry_num = retry_num + 1
            else:  # 超时并超过重试次数，程序结束跳出循环，并认为页面已经加载完毕！
                print("滚动条已经处于页面最下方！")
                # 滚动条调整至页面顶部
                self.driver.execute_script('window.scrollTo({top: 0,behavior: "smooth"})')
                return True

    def download_news_img(self, html, response, news_content_html):
        """
        @summary: 下载新闻详情页图片
        @param html: 新闻内容 etree.HTML 元素
        @param response: response.url  新闻详情页URL
        @param news_content_html: 新闻内容 文本HTML 元素
        @return: news_content_html(处理完图片src='***'之后的文本HTML) 、standard_img_url_url(图片列表集合)
        """
        # 本篇新闻的图片集合
        standard_img_url_url = []
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

            img_response = self.request.get(standard_img_url, timeout=10,
                                            headers={'user-agent': random.choice(self.settings.get('USER_AGENT_LIST'))},
                                            verify=False)
            if img_response.status_code == 200:
                # 取图片后缀
                # 不带图片后缀 默认jpg
                # http://p9.pstatp.com/large/pgc-image/6f6765ccd6fa4e0195e4b8152dd1fdad
                if standard_img_url.split('/')[-1] not in '.':
                    img_postfix = 'jpg'
                else:
                    img_postfix = standard_img_url.split('.')[-1]
                    if img_postfix:
                        img_postfix_value = re.findall('(.*?)[^a-zA-Z0-9_]', img_postfix)
                        if img_postfix_value:
                            img_postfix = img_postfix_value[0]

                img_full_name = 'newsimg_' + time.strftime("%Y_%m_%d_") + \
                                str(int(round(time.time() * 1000))) + '.' + img_postfix
                # 将图片内容转换为 Bytes
                bytes_img_content = io.BytesIO(img_response.content)
                # 过滤小于1024 Bytes的图片
                if len(bytes_img_content.getvalue()) > 1024:
                    # print(standard_img_url)
                    standard_img_url_url.append(standard_img_url)
                    self.minio.put_object(bucket_name=self.minio_bucket_name, object_name=img_full_name,
                                          data=bytes_img_content,
                                          length=-1, content_type='image/png', part_size=10 * 1024 * 1024)
                    # 替换HTML中的图片URL地址为  桶名+文件名
                    news_content_html = news_content_html.replace(img_url.replace('&', '&amp;'),
                                                                  "/" + self.minio_bucket_name + "/" + img_full_name)
            else:
                print(standard_img_url)
                print(img_response.status_code)
        return news_content_html, standard_img_url_url
