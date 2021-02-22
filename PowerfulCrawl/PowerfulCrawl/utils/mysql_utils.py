# -*- coding:utf-8 -*-
# Author      : ZuoWei<ZuoWei@yuchen.net.cn>
# Datetime    : 2021-01-01 12:00
# User        : ZuoWei
# Product     : PyCharm
# Project     : Powerful_Crawl
# File        : mysql_utils.py
# Description : 数据库连接池工具类
import pymysql

from dbutils.pooled_db import PooledDB
from scrapy.utils.project import get_project_settings


class MySQLUtils:

    def __init__(self):
        self.settings = get_project_settings()
        # 连接数据库
        self.POOL = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,
            # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少， _maxcached 永远为0，所以永远是所有链接都共享。
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set date style to ...", "set time zone ..."]
            ping=4,
            # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            host=self.settings.get('MYSQL_HOST'),
            port=self.settings.get('MYSQL_PORT'),
            user=self.settings.get('MYSQL_USER'),
            password=self.settings.get('MYSQL_PASS'),
            database=self.settings.get('MYSQL_DATABASE'),
            charset='utf8'
        )

    def connect(self):
        conn = self.POOL.connection()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        return conn, cursor

    @staticmethod
    def connect_close(cursor, conn):
        cursor.close()
        conn.close()

    def fetch_one(self, sql, args=None):
        conn, cursor = self.connect()
        if args is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, args)
        data = cursor.fetchone()
        self.connect_close(cursor, conn)
        return data

    def fetch_all(self, sql, args=None):
        conn, cursor = self.connect()
        if args is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, args)
        data_list = cursor.fetchall()
        self.connect_close(cursor, conn)
        return data_list

    def insert(self, sql, args=None):
        conn, cursor = self.connect()
        if args is None:
            row = cursor.execute(sql)
        else:
            row = cursor.execute(sql, args)
        conn.commit()
        self.connect_close(cursor, conn)
        return row

    def update(self, sql, args=None):
        conn, cursor = self.connect()
        if args is None:
            row = cursor.execute(sql)
        else:
            row = cursor.execute(sql, args)
        conn.commit()
        self.connect_close(cursor, conn)
        return row

    def begin(self):
        """
        @summary: 开启事务
        """
        conn, cursor = self.connect()
        conn.autocommit(0)

    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        conn, cursor = self.connect()
        if option == 'commit':
            conn.autocommit()
        else:
            conn.rollback()
        self.connect_close(cursor, conn)

    def dispose(self, is_end=1):
        """
        @summary: 释放连接池资源
        """
        if is_end == 1:
            self.end('commit')
        else:
            self.end('rollback')
