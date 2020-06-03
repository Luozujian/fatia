#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/11 17:41
# @Author  : luozujian
# 消费PageLink，生产detail
import pymysql
import bs4
import pika
import requests
import re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

class DetailProducer:
    def __init__(self ):
        # rabbitmq producer 信息设置
        self.credentials = pika.PlainCredentials('guest', 'guest')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='movie', exchange_type='direct')
        self.channel.queue_declare(queue="pageLink", durable=True)
        self.channel.queue_declare(queue="detail", durable=True)
        self.channel.queue_bind(exchange="movie", queue="pageLink", routing_key="pageLink")
        self.channel.queue_bind(exchange="movie", queue="detail", routing_key="detail")


        # 设置线程池大小
        self.pool = ThreadPoolExecutor(5)

        # 设置头
        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36"
        }

        # 站点
        self.baseUrl = "https://www.imdb.com"

    def callBack(self,channel,method,properity,body):
        try:
            pageHtml = requests.get(body,headers = self.headers)
            bs4 = BeautifulSoup(pageHtml.text,"lxml")
            nextBar = bs4.find("a", {"href": re.compile(".*ref_=adv_nxt")})
            nextUrl = self.baseUrl + nextBar.attrs['href']
            self.channel.basic_publish(exchange="movie", routing_key="pageLink", body=nextUrl,
                                       properties=pika.BasicProperties(
                                           delivery_mode=2,  # 消息持久化
                                       ))

            itemLinks = bs4.findAll("a",{"href":re.compile("/title/.*/?ref_=adv_li_i")})
            for link in itemLinks:
                itemLink = self.baseUrl + link.attrs["href"]
                self.channel.basic_publish(exchange="movie", routing_key="detail", body=itemLink,
                                           properties=pika.BasicProperties(
                                               delivery_mode=2,  # 消息持久化
                                           ))
                print(itemLink)

        except Exception as e:
            print(e)
            pass
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(on_message_callback = self.callBack,queue="pageLink")
            self.channel.start_consuming()
        except Exception as e:
            print(e)

