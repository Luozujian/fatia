#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/24 19:03
# @Author  : luozujian
import pika
import requests
from bs4 import BeautifulSoup
import re


class DetailTvProducer:
    def __init__(self):
        # 设置初始url
        self.orginUrl = "https://www.imdb.com/feature/genre/?ref_=nv_ch_gr"

        # rabbitmq producer 信息设置
        self.credentials = pika.PlainCredentials('guest', 'guest')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='tv', exchange_type='direct')
        self.channel.queue_declare(queue="tv_pageLink", durable=True)
        self.channel.queue_declare(queue="tv_detail", durable=True)
        self.channel.queue_bind(exchange="tv", queue="tv_pageLink", routing_key="tv_pageLink")
        self.channel.queue_bind(exchange="tv", queue="tv_detail", routing_key="tv_detail")

        # 设置头
        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36"
        }

        # 站点
        self.baseUrl = "https://www.imdb.com"
        pass



    def callBack(self,channel,method,properity,body):
        print("detailTvProducer: ",body)
        try:
            pageHtml = requests.get(body, headers=self.headers)
            bs4 = BeautifulSoup(pageHtml.text, "lxml")
            nextBar = bs4.find("a", {"href": re.compile(".*ref_=adv_nxt")})
            nextUrl = self.baseUrl + nextBar.attrs['href']

            #如果没有ack就不会继续消费
            print("detailTvProducer-push: ",nextUrl)
            self.channel.basic_publish(exchange="tv", routing_key="tv_pageLink", body=nextUrl,
                                       properties=pika.BasicProperties(
                                           delivery_mode=2,  # 消息持久化
                                       ))

            itemLinks = bs4.findAll("a", {"href": re.compile("/title/.*/?ref_=adv_li_i")})
            for link in itemLinks:
                itemLink = self.baseUrl + link.attrs["href"]
                self.channel.basic_publish(exchange="tv", routing_key="tv_detail", body=itemLink,
                                           properties=pika.BasicProperties(
                                               delivery_mode=2,  # 消息持久化
                                           ))

        except Exception as e:
            print(e)

        print("detailTvProducer: ack")
        channel.basic_ack(delivery_tag=method.delivery_tag)



    def start(self):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(on_message_callback = self.callBack,queue="tv_pageLink")
            self.channel.start_consuming()
        except Exception as e:
            print(e)

if __name__ == '__main__':
    app = DetailTvProducer()
    app.start()