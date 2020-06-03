#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/11 10:54
# @Author  : luozujian
#将基础连接放入mq当中
import pymysql
import bs4
import pika
import requests
import re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup


class ImdbSpider:


    def __init__(self):
        # 设置初始url
        self.orginUrl = "https://www.imdb.com/feature/genre/?ref_=nv_ch_gr"

        # rabbitmq producer 信息设置
        self.credentials = pika.PlainCredentials('guest','guest')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='movie' , exchange_type='direct')
        self.channel.queue_declare(queue="pageLink",durable=True)
        self.channel.queue_declare(queue="detail",durable=True)
        self.channel.queue_bind(exchange="movie",queue="pageLink",routing_key="pageLink")
        self.channel.queue_bind(exchange="movie", queue="detail", routing_key="detail")

        # self.channel.basic_publish(exchange="movie",routing_key="pagelink",body="test",properties=pika.BasicProperties(
        #     delivery_mode=2,
        # ))



        #设置线程池大小
        self.pool = ThreadPoolExecutor(5)

        #设置头
        self.headers = {
            "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36"
        }

        #站点
        self.baseUrl = "https://www.imdb.com"

    #获取翻页链接
    def getPageUrl(self,orginPageUrl):
        pageHtml = requests.get(orginPageUrl,headers = self.headers)
        bs4 = BeautifulSoup(pageHtml.text,"html.parser")
        nextBar = bs4.find("a",{"href":re.compile(".*ref_=adv_nxt")})
        nextUrl = self.baseUrl + nextBar.attrs['href']
        pass



    #获取原始链接
    def getKindUrl(self,orginUrl):
        rq = requests.get(orginUrl,headers = self.headers)
        bs4 = BeautifulSoup(rq.text, "html.parser")
        #print(bs4.prettify())
        count = 0
        itemLinks = bs4.findAll("a",{"href":re.compile("/search/title.genres=.{0,20}&title_type=feature.*explore=genres")})
        for itemLink in itemLinks:
            newLink = self.baseUrl + itemLink.attrs['href']
            self.channel.basic_publish(exchange="movie",routing_key="pageLink",body=newLink,properties=pika.BasicProperties(
                delivery_mode=2,#消息持久化
            ))
            count = count+1
        print(count)


    def start(self):
        try:
            self.getKindUrl(self.orginUrl)
        except Exception as e:
            print(e)






#






# conn = pymysql.connect(host = '127.0.0.1',port = 3306,user='root',passwd='luozujian',db='fatia')
# cur = conn.cursor()
# cur.execute('use fatia')
#
#
#
#
# cur.close()
# conn.close()

