#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/11 18:24
# @Author  : luozujian
import pymysql
import bs4
import pika
import requests
import re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup


class DetailConsumer:

    def __init__(self):
        # rabbitmq producer 信息设置
        self.credentials = pika.PlainCredentials('guest', 'guest')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='movie', exchange_type='direct')
        self.channel.queue_declare(queue="pageLink", durable=True)
        self.channel.queue_declare(queue="detail", durable=True)
        self.channel.queue_bind(exchange="movie", queue="pageLink", routing_key="pageLink")
        self.channel.queue_bind(exchange="movie", queue="detail", routing_key="detail")

        # 设置头
        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36"
        }

        # 站点
        self.baseUrl = "https://www.imdb.com"

        #mysql配置
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='luozujian', db='fatia')
        self.cur = self.conn.cursor()


        #设置线程池
        self.pool = ThreadPoolExecutor(5)

    def callBack(self, channel, method, properity, body):
        try:
            detailHtml = requests.get(body,headers = self.headers)
            bs4 = BeautifulSoup(detailHtml.text,"lxml")

            #电影名字
            name = bs4.find("div",{"class":"title_wrapper"}).h1.get_text()
            items = bs4.findAll("div",{"class","txt-block"})
            # 上映时间
            releaseTime = ""
            # 类型
            type = ""
            typeLinks = bs4.find("div",{"id":"titleStoryLine"}).findAll("a",{"href":re.compile(".*ref_=tt_ov_inf")})
            for aLink in typeLinks:
                text = aLink.get_text().strip()
                if len(type) == 0:
                    type = text
                else:
                    type = type + "," + text

            # 制片地区
            area = ""
            # 导演
            director = ""
            # 演员
            actor = ""
            templinks = bs4.findAll("div", {"class": "credit_summary_item"})
            for templink in templinks:
                text = templink.get_text().strip()
                if "Director" in text:
                    director = text
                elif "Stars" in text:
                    actor = text

            # 投资
            investment = ""
            # 市场
            # 是否知名小说改编
            # 制片方
            producer = ""
            # 观影人员类型
            # 票房
            boxOffice = ""
            # 热度
            # 评分
            score = bs4.find("span",{"itemprop":"ratingValue"}).get_text().strip()
            # 链接
            # 时长
            time = ""
            for item in items:
                text = item.get_text()
                if "Release" in text:
                    releaseTime = text
                elif "Country" in text:
                    area = text
                elif "Budget" in text:
                    investment = text
                elif "Cumulative" in text:
                    boxOffice = text
                elif "Runtime" in text:
                    time = text
                elif "Production" in text:
                    producer = text
            sql = 'select * from fatia_new where name = "%s" ' % (name)
            self.cur.execute(sql)
            dataTemp = self.cur.fetchone()
            if dataTemp is None or len(dataTemp) == 0:
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            sql = "insert into fatia_new(name,release_time,type,area,director,actor,investment,producer,box_office,score,url,time)" \
                  " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            self.cur.execute(sql,(name,releaseTime,type,area,director,actor,investment,producer,boxOffice,score,body,time))
            self.conn.commit()

        except Exception as e:
            print(body)
            print(e)

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        try:
            self.channel.basic_qos(prefetch_count=10)
            self.channel.basic_consume(on_message_callback=self.callBack, queue="detail")
            self.channel.start_consuming()
        except Exception as e:
            print(e)
