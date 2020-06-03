#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/24 16:33
# @Author  : luozujian
# 爬取影视模块

import pika
import requests
from bs4 import BeautifulSoup
import re

class ImdbTvSpider:

    def __init__(self):
        # 设置初始url
        self.orginUrl = "https://www.imdb.com/feature/genre/?ref_=nv_ch_gr"

        # rabbitmq producer 信息设置
        self.credentials = pika.PlainCredentials('guest', 'guest')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='tv' , exchange_type='direct')
        self.channel.queue_declare(queue="tv_pageLink",durable=True)
        self.channel.queue_declare(queue="tv_detail",durable=True)
        self.channel.queue_bind(exchange="tv",queue="tv_pageLink",routing_key="tv_pageLink")
        self.channel.queue_bind(exchange="tv", queue="tv_detail", routing_key="tv_detail")

        # 设置头
        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36"
        }

        # 站点
        self.baseUrl = "https://www.imdb.com"

    def getKindUrl(self,orginUrl):
        rq = requests.get(orginUrl, headers=self.headers)
        bs4 = BeautifulSoup(rq.text, "html.parser")
        #print(bs4.prettify())
        itemLinks = bs4.findAll("a",{"href":re.compile("/search/title.genres=.{0,20}&title_type=tv_series.*")})
        for item in itemLinks:
            pageUrl = self.baseUrl + item.attrs['href']
            self.channel.basic_publish(exchange="tv", routing_key="tv_pageLink", body=pageUrl,
                                       properties=pika.BasicProperties(
                                           delivery_mode=2,  # 消息持久化
                                       ))



    def start(self):
        try:
            self.getKindUrl(self.orginUrl)
        except Exception as e:
            print(e)



if __name__ == '__main__':
    app = ImdbTvSpider()
    app.start()