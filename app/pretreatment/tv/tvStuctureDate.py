#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/26 9:24
# @Author  : luozujian
# 对tv的数据进行结构化
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from scipy import spatial
from sqlalchemy import create_engine
import os
import pymysql

class TvStuctureDate:
    def __init__(self):
        engin = create_engine("mysql+pymysql://root:luozujian@localhost:3306/fatia", encoding='utf-8')
        sql = 'select * from fatia_tv_copy1'

        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='luozujian', db='fatia')
        self.cur = self.conn.cursor()

        self.tv = pd.read_sql_query(sql, con=engin)
        #self.tv = self.tv.head(100)


        #清空结构化数据库的数据
        self.deleteStuctureOfTv()
        #清洗数据
        self.handleDate()
        pass

    def predict(self,predict):
        if predict is not None:
            self.tv = self.tv.append(predict,ignore_index=True)
        self.tv['vote_average'] = self.tv['vote_average'].astype(float)
        self.tvStutrue = self.tv


    #清空结构化数据库的数据
    def deleteStuctureOfTv(self):
        sql = "delete from fatia_tv_stucture"
        self.cur.execute(sql)
        self.conn.commit()

    # 对空的数据进行清洗
    def handleDate(self):
        for index, row in self.tv.iteritems():
            directorSerie = self.tv[index]
            directorSerie[directorSerie == ""] = np.nan
            self.tv[index] = directorSerie
        self.tv = self.tv.dropna(axis=0, how='any')
        self.tv['vote_average'] = self.tv['vote_average'].astype(float)
        self.tv = self.tv.head(15000)
        self.tvStutrue = self.tv
        pass

    #进行演员展开处理
    def handleActors(self):
        movieTemp = pd.DataFrame(columns=['tv_id','actor','types','director','vote_average','hot','onehot','time'])
        for i ,row in self.tv.iterrows():
            actors = row['actors'].split(',')
            for actor in actors:
                dicTemp = {"tv_id":row['id'],"types":row['types'],'actor':actor,"director":row['director'],'vote_average':row['vote_average'],'hot':row['hot'],"time":row['time']}
                movieTemp = movieTemp.append(dicTemp,ignore_index=True)
        self.tvStutrue = movieTemp
        pass

    # 给导演按照平均评分划分等级
    def differDirectorRevenue(self):
        print("differDirectorRevenue")
        pdTemp = self.tvStutrue['director'].value_counts()

        map = dict()

        moviesTemp = pd.DataFrame(columns=['director', 'vote_average'])

        for index, row in self.tvStutrue.iterrows():
            if pdTemp[row['director']] >= 2:
                moviesTemp = moviesTemp.append(
                    [{'director': row['director'], 'vote_average': row['vote_average']}], ignore_index=True)

        moviesTemp = moviesTemp.groupby(['director'], as_index=False)['vote_average'].mean().sort_values(
            'vote_average', ascending=False)
        moviesTemp['level'] = None

        base = int(moviesTemp.shape[0]) / 5
        index = 1
        for i, row in moviesTemp.iterrows():
            if index <= 1 * base:
                moviesTemp.loc[i, 'level'] = '1'
                map[row['director']] = '1'
            elif index <= 2 * base:
                moviesTemp.loc[i, 'level'] = '2'
                map[row['director']] = '2'
            elif index <= 3 * base:
                moviesTemp.loc[i, 'level'] = '3'
                map[row['director']] = '3'
            elif index <= 4 * base:
                moviesTemp.loc[i, 'level'] = '4'
                map[row['director']] = '4'
            elif index <= 5 * base:
                moviesTemp.loc[i, 'level'] = '5'
                map[row['director']] = '5'
            index = index + 1

        self.tvStutrue['directorlevel'] = None

        deleteItems = []
        for i, row in self.tvStutrue.iterrows():
            if map.get(row['director']) is None:
                deleteItems.append(i)
            else:
                self.tvStutrue.loc[i, 'directorlevel'] = map.get(row['director'])
        self.tvStutrue = self.tvStutrue.drop(index=deleteItems)
        pass

    # 不同演员的总票房
    def diiferActorRevenue(self):
        #self.differDirectorRevenue()

        movieTemp = pd.DataFrame(columns=['actor', 'vote_average'])

        for i, row in self.tvStutrue.iterrows():
            movieTemp = movieTemp.append([{'actor': row['actor'], 'vote_average': row['vote_average']}])

        movieTemp = movieTemp.groupby("actor", as_index=False)['vote_average'].mean().sort_values('vote_average',
                                                                                                  ascending=False)
        base = int(movieTemp.shape[0]) / 5
        index = 1
        map = dict()

        self.tvStutrue['actorlevel'] = None

        for i, row in movieTemp.iterrows():
            print(index)
            if index <= 1 * base:
                self.tvStutrue.loc[i, 'actorlevel'] = '1'
                map[row['actor']] = '1'
            elif index <= 2 * base:
                self.tvStutrue.loc[i, 'actorlevel'] = '2'
                map[row['actor']] = '2'
            elif index <= 3 * base:
                self.tvStutrue.loc[i, 'actorlevel'] = '3'
                map[row['actor']] = '3'
            elif index <= 4 * base:
                self.tvStutrue.loc[i, 'actorlevel'] = '4'
                map[row['actor']] = '4'
            elif index <= 5 * base:
                self.tvStutrue.loc[i, 'actorlevel'] = '5'
                map[row['actor']] = '5'
            index = index + 1

        deleteItems = []
        for i, row in self.tvStutrue.iterrows():
            if map.get(row['actor']) is None:
                deleteItems.append(i)
            else:
                self.tvStutrue.loc[i, 'actorlevel'] = map.get(row['actor'])
        self.tvStutrue = self.tvStutrue.drop(index=deleteItems)
        pass

    #进行oneHot编码
    def oneHot(self):

        for i, row in self.tvStutrue.iterrows():
            typeOneHot = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,0,0,0,0]
            types = row['types'].split(',')
            if 'Action' in row['types']:
                typeOneHot[0] = 1
            if 'Adventure' in row['types']:
                typeOneHot[1] = 1
            if 'Animation' in row['types']:
                typeOneHot[2] = 1
            if 'Biography' in row['types']:
                typeOneHot[3] = 1
            if 'Comedy' in row['types']:
                typeOneHot[4] = 1
            if 'Crime' in row['types']:
                typeOneHot[5] = 1
            if 'Documentary' in row['types']:
                typeOneHot[6] = 1
            if 'Drama' in row['types']:
                typeOneHot[7] = 1
            if 'Family' in row['types']:
                typeOneHot[8] = 1
            if 'Fantasy' in row['types']:
                typeOneHot[9] = 1
            if 'Game Show' in row['types']:
                typeOneHot[10] = 1
            if 'History' in row['types']:
                typeOneHot[11] = 1
            if 'Horror' in row['types']:
                typeOneHot[12] = 1
            if 'Music' in row['types']:
                typeOneHot[13] = 1
            if 'Musical' in row['types']:
                typeOneHot[14] = 1
            if 'Mystery' in row['types']:
                typeOneHot[15] = 1
            if 'News' in row['types']:
                typeOneHot[16] = 1
            if 'Reality-TV' in row['types']:
                typeOneHot[17] = 1
            if 'Romance' in row['types']:
                typeOneHot[18] = 1
            if 'Sci-Fi' in row['types']:
                typeOneHot[19] = 1
            if 'Sport' in row['types']:
                typeOneHot[20] = 1
            if 'Superhero' in row['types']:
                typeOneHot[21] = 1
            if 'Talk Show' in row['types']:
                typeOneHot[22] = 1
            if 'Thriller' in row['types']:
                typeOneHot[23] = 1
            if 'War' in row['types']:
                typeOneHot[24] = 1
            if 'Western' in row['types']:
                typeOneHot[25] = 1
            self.tvStutrue.loc[i, 'onehot'] = ','.join(str(i) for i in typeOneHot)
        pass


    #将结构化的数据插入数据库
    def insertIntoDataBase(self):
        for i , row in self.tvStutrue.iterrows():
            sql = 'insert into fatia_tv_stucture (tv_id,actor_lever,director_lever,vote_average,hot,onehot,time) values("%s","%s","%s","%s","%s","%s","%s")' % (row['tv_id'],row['actorlevel'],row['directorlevel'],row['vote_average'],row['hot'],row['onehot'],row['time'])
            self.cur.execute(sql)
            self.conn.commit()
        pass



    def start(self):

        self.handleActors()
        self.differDirectorRevenue()
        self.diiferActorRevenue()
        self.oneHot()
        self.insertIntoDataBase()
        pass



if __name__ == "__main__":
    date = {
        "id":"-1",
        "title": "The Last Kingdom",
        "types": "Action,Drama,History",
        "time": "132",
        "director": "Michael Dante DiMartino",
        "actors": "Alexander Dreymon, Eliza Butterworth, Arnas Fedaravicius",
        "area": "USA",
        "vote_average":"8.7",
        "hot":"10000"
    }
    app = TvStuctureDate()
    app.predict(date)
    app.start()