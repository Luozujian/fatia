#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/24 19:13
# @Author  : luozujian
import pika
import requests
from bs4 import BeautifulSoup
import re
import pymysql
import datetime



class DetailTvConsumer:

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

        # mysql配置
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='luozujian', db='fatia')
        self.cur = self.conn.cursor()

        # 站点
        self.baseUrl = "https://www.imdb.com"
        pass

    def extraTime(self,realse_time):
        if '(' in realse_time:
            index = realse_time.index('(')
            realse_time = realse_time[:index]
        realse_time = realse_time[14:].strip()
        try:
            time_format = datetime.datetime.strptime(realse_time, '%d %B %Y')
        except Exception as e:
            time_format = datetime.datetime.strptime(realse_time, '%B %Y')
        return datetime.datetime.strftime(time_format, '%Y-%m')

    def extraDirector(self, director):
        if director is None or len(director) == 0:
            return ""
        if '(' in director:
            index = director.index('(')
            director = director[:index]
        index = director.index(":")
        director = director[index + 1:].strip()
        return director.split(",")[0]

    def extraActors(self, actors):
        if ':' in actors:
            index = actors.index(':')
            actors = actors[index + 1:]
        if '|' in actors:
            index = actors.index('|')
            actors = actors[:index]
        return actors.strip()


    def callBack(self,channel,method,properity,body):
        try:
            detailHtml = requests.get(body, headers=self.headers)
            bs4 = BeautifulSoup(detailHtml.text, "lxml")

            #电视剧名称
            name = bs4.find("div", {"class": "title_wrapper"}).h1.get_text()

            # 类型
            type = ""
            typeLinks = bs4.find("div", {"id": "titleStoryLine"}).findAll("a", {"href": re.compile(".*ref_=tt_ov_inf")})
            for aLink in typeLinks:
                text = aLink.get_text().strip()
                if len(type) == 0:
                    type = text
                else:
                    type = type + "," + text

            # 演员
            actor = ""
            #导演
            director = ""

            templinks = bs4.findAll("div", {"class": "credit_summary_item"})
            for templink in templinks:
                text = templink.get_text().strip()
                if "Creator" in text:
                    director = text
                elif "Stars" in text:
                    actor = text

            #评分
            score = bs4.find("span", {"itemprop": "ratingValue"}).get_text().strip()

            #热度
            hot = bs4.find("span", {"itemprop": "ratingCount"}).get_text().strip()

            #产地
            area = ""

            #链接
            url = body

            #每集时长
            time = ""

            #release_time
            release_time = ""


            #生产商
            producer = ""

            items = bs4.findAll("div", {"class", "txt-block"})
            for item in items:
                text = item.get_text()
                if "Country" in text:
                    area = text
                if "Runtime" in text:
                    time = text
                if "Release Date" in text:
                    release_time = text
                if "Production Co" in text:
                    producer = text


            time = re.findall("\d+",time)[0]
            area = area.replace("\r\n","").strip()[9:]
            release_time = self.extraTime(release_time)
            director = self.extraDirector(director)
            actor = self.extraActors(actor)

            updatetime = datetime.date.today()
            sql = 'insert into fatia_tv_copy1(title,vote_average,actors,director,types,area,release_time,producer,hot,time,url,updatetime) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' % (name,score,actor,director,type,area,release_time,producer,hot,time,body,updatetime)
            self.cur.execute(sql)
            self.conn.commit()

        except Exception as e:
            print(e)
            pass

        channel.basic_ack(delivery_tag=method.delivery_tag)


    def start(self):
        try:
            self.channel.basic_qos(prefetch_count=10)
            self.channel.basic_consume(on_message_callback=self.callBack, queue="tv_detail")
            self.channel.start_consuming()
        except Exception as e:
            print(e)


if __name__ == "__main__":
    app = DetailTvConsumer()
    app.start()