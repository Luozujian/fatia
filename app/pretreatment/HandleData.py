#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/21 22:03
# @Author  : luozujian
import pymysql
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re



#统一货币
class HandleNumber:

    def __init__(self):
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='luozujian', db='fatia')
        self.cur = self.conn.cursor()
        pass

    def start(self):
        sql = "select * from fatia_1"
        self.cur.execute(sql)
        datas = self.cur.fetchall()
        set = {
            "EGP":0.06341,
            "GPB":1.2495,
            "CAD":0.7095,
            "INR":0.01301,
            "DKK":0.1458,
            "ZAR":0.0538,
            "MXN":0.0414,
            "CYN":0.1414,
            "ADU":0.6312,
            "EUR":1.0876,
            "KRW":0.0008,
            "GBP":1.2495,
            "PHP":0.01960,
            "JPY":0.009299,
            "PLN":0.2398,
            "NOK":0.0956,
            "BRL":0.1905,
            "AUD":0.6312,
            "SEK":0.09997,
            "ARS":0.01523,
            "SGD":0.7007,
            "$":1
        }



        for data in datas:
            investment = 0
            boxOffice = 0
            flag = 1
            for item in set:
                if item in data[7]:
                    investment = int("".join(re.findall("\d+", str(data[7]))))
                    investment = investment * set.get(item)
                    flag = flag + 1
                if item in data[12]:
                    boxOffice = int("".join(re.findall("\d+", str(data[12]))))
                    boxOffice = boxOffice * set.get(item)
                    flag = flag + 1
            if flag != 3:
                continue

            time  = "".join(re.findall("Runtime:[0-9]{2,3}",data[16].replace("\n","")))
            time = time[8:]


            sql = "insert into fatia_2(name,release_time,type,area,director,actor,investment,producer,box_office,score,url,time)" \
                  " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cur.execute(sql, (
            data[1], data[2], data[3], data[4], data[5], data[6], investment, data[10], boxOffice, data[14], data[15], time))
            self.conn.commit()




#格式化，上映时间，导演和演员
class FormateDate:
    def __init__(self):
        self.set = set()
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='luozujian', db='fatia')
        self.cur = self.conn.cursor()
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

    def extraYear(self,realse_time):
        if '(' in realse_time:
            index = realse_time.index('(')
            realse_time = realse_time[:index]
        realse_time = realse_time[14:].strip()
        try:
            time_format = datetime.datetime.strptime(realse_time, '%d %B %Y')
        except Exception as e:
            time_format = datetime.datetime.strptime(realse_time, '%B %Y')
        return datetime.datetime.strftime(time_format, '%Y')


    def extraDirector(self,director):
        if '(' in director:
            index = director.index('(')
            director = director[:index]
        index = director.index(":")
        director = director[index+1:].strip()
        return director.split(",")[0]

    def extraActors(self,actors):
        if ':' in actors:
            index = actors.index(':')
            actors = actors[index+1:]
        if '|' in actors:
            index = actors.index('|')
            actors = actors[:index]
        return actors.strip()


    def start(self):
        sql = 'select * from fatia_2'
        self.cur.execute(sql)
        datas = self.cur.fetchall()
        title = ''
        release_time = ''
        genres = ''
        area = ''
        director = ''
        actors = ''
        budget = ''
        market = ''
        flag = ''
        producer = ''
        audience_type = ''
        revenue = ''
        heat = ''
        vote_average = ''
        url = ''
        time = ''
        year = ''
        for data in datas:
            if 'Release Date' not in data[2]:
                continue
            if 'Director' not in data[5]:
                continue
            if 'Stars' not in data[6]:
                continue


            title = data[1]
            try:
                release_time = self.extraTime(data[2])
            except Exception:
                continue

            genres = data[3]
            area = data[4]
            director = self.extraDirector(data[5])
            actors = self.extraActors(data[6])
            budget = data[7]
            producer = data[10]
            revenue = data[12]
            vote_average = data[14]
            url = data[15]
            time = data[16]
            year = self.extraYear(data[2])




            if actors == None:
                continue


            if len(actors) * len(director) * len(revenue) * len(budget)*len(genres)*len(release_time) == 0:
                continue

            if title in self.set:
                continue
            else:
                self.set.add(title)


            sql = "insert into fatia_3(title,release_time,genres,area,director,actors,budget,market,flag,producer,audience_type,revenue,heat,vote_average,url,time,year) " \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            self.cur.execute(sql, (
            title, release_time, genres, area, director, actors, budget, market, flag, producer, audience_type, revenue,
            heat, vote_average, url, time, year))
            self.conn.commit()
            pass
