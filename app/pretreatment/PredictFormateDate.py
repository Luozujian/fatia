#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/28 19:34
# @Author  : luozujian


# 为了预测格式化数据
import pymysql
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from sqlalchemy import create_engine



#分等级
class PredictFormateDate:

    def __init__(self):
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='luozujian', db='fatia')
        self.cur = self.conn.cursor()
        pd.set_option('display.max_columns', 1000)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', 1000)


        engin = create_engine("mysql+pymysql://root:luozujian@localhost:3306/fatia", encoding='utf-8')
        sql = 'select * from fatia_3'

        self.movies = pd.read_sql_query(sql, con=engin)
        self.movies = self.movies.drop_duplicates(['title'], keep='first')
        self.movies = self.movies.drop(['id', 'area', 'market', 'flag', 'producer', 'audience_type', 'heat', 'url'],
                                       axis=1)
        self.movies.year = self.movies.year.astype('int')
        self.movies.budget = self.movies.budget.astype('float')
        self.movies.revenue = self.movies.revenue.astype('float')
        self.movies.vote_average = self.movies.vote_average.astype('float')
        self.movies_15 = self.movies[(self.movies.year >= 2005) & (self.movies.year < 2020) & (
                    self.movies.budget * self.movies.revenue * self.movies.vote_average != 0)]
        pass

    #不同导演的平均评分
    def differDirectorRevenue(self):
        print("differDirectorRevenue")
        pdTemp = self.movies_15['director'].value_counts()

        map = dict()

        self.movies1 = pd.DataFrame(columns=['director','vote_average'])

        for index,row in self.movies_15.iterrows():
            if pdTemp[row['director']] >= 2:
                self.movies1 = self.movies1.append([{'director':row['director'],'vote_average':row['vote_average']}],ignore_index=True)

        self.movies1 = self.movies1.groupby(['director'],as_index=False)['vote_average'].mean().sort_values('vote_average',ascending=False)
        self.movies1['level'] = None

        index = 1
        for i,row in self.movies1.iterrows():
            if index <= 1*667:
                self.movies1.loc[i,'level'] = '1'
                map[row['director']] = '1'
            elif index <= 2*667:
                self.movies1.loc[i,'level'] = '2'
                map[row['director']] = '2'
            elif index <= 3*667:
                self.movies1.loc[i,'level'] = '3'
                map[row['director']] = '3'
            elif index <= 4*667:
                self.movies1.loc[i,'level']=  '4'
                map[row['director']] = '4'
            elif index <= 5*667:
                self.movies1.loc[i,'level'] = '5'
                map[row['director']] = '5'
            index = index + 1

        self.movies_15['level'] = None

        deleteItems = []
        for i,row in self.movies_15.iterrows():
            if map.get(row['director']) is None:
                deleteItems.append(i)
            else:
                self.movies_15.loc[i,'level'] = map.get(row['director'])
        self.movies_15 = self.movies_15.drop(index=deleteItems)
        pass


    #不同演员的总票房
    def diiferActorRevenue(self):
        print("diiferActorRevenue")
        self.differDirectorRevenue()

        self.movie2 = pd.DataFrame(columns=['title','release_time','genres','actor','budget','revenue','time','year','director','vote_average','directorlever','actorlever'])

        self.temp = pd.DataFrame(columns=['actor','vote_average'])

        for i,row in self.movies_15.iterrows():
            actorList = row['actors'].split(',')
            for item in actorList:
                actor = item.strip()
                self.temp = self.temp.append([{'actor':actor,'vote_average':row['vote_average']}])
        pass
        self.temp = self.temp.groupby("actor",as_index=False)['vote_average'].mean().sort_values('vote_average',ascending=False)
        print("temp = ",self.temp.shape[0])
        base = int(self.temp.shape[0]) / 5
        index = 1
        map = dict()
        for i,row in self.temp.iterrows():
            if index <= 1*base:
                self.movies1.loc[i,'level'] = '1'
                map[row['actor']] = '1'
            elif index <= 2*base:
                self.movies1.loc[i,'level'] = '2'
                map[row['actor']] = '2'
            elif index <= 3*base:
                self.movies1.loc[i,'level'] = '3'
                map[row['actor']] = '3'
            elif index <= 4*base:
                self.movies1.loc[i,'level']=  '4'
                map[row['actor']] = '4'
            elif index <= 5*base:
                self.movies1.loc[i,'level'] = '5'
                map[row['actor']] = '5'
            index = index + 1

        deleteItems = []
        for i ,row in self.movies_15.iterrows():
            actorList = row['actors'].split(',')
            for item in actorList:
                actor = item.strip()
                if map.get(actor) is None:
                    deleteItems.append(i)
                else:
                    self.movie2 = self.movie2.append(
                        [{
                            'title':row['title'],
                            'release_time':row['release_time'],
                            'genres': row['genres'],
                            'actor':actor,
                            'budget':row['budget'],
                            'revenue':row['revenue'],
                            'time':row['time'],
                            'year':row['year'],
                            'director':row['director'],
                            'vote_average':row['vote_average'],
                            'directorlever':row['level'],
                            'actorlever': map.get(actor)
                        }]
                    )
        self.movies_15 = self.movie2.drop(index=deleteItems)
        pass

    def redExcel(self):
        print("redExcel")
        self.movies_15['typeOneHot'] = None
        self.movies_15['month'] = None
        index = 1
        for i,row in self.movies_15.iterrows():
            typeOneHot = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
            types = row['genres'].split(',')
            if 'Sci-Fi' in row['genres']:
                typeOneHot[0] = 1
            if 'Biography' in row['genres']:
                typeOneHot[1] = 1
            if 'Mystery' in row['genres']:
                typeOneHot[2] = 1
            if 'Western' in row['genres']:
                typeOneHot[3] = 1
            if 'Musical' in row['genres']:
                typeOneHot[4] = 1
            if 'Thriller' in row['genres']:
                typeOneHot[5] = 1
            if 'Music' in row['genres']:
                typeOneHot[6] = 1
            if 'Crime' in row['genres']:
                typeOneHot[7] = 1
            if 'Animation' in row['genres']:
                typeOneHot[8] = 1
            if 'Adult' in row['genres']:
                typeOneHot[9] = 1
            if 'Adventure' in row['genres']:
                typeOneHot[10] = 1
            if 'Fantasy' in row['genres']:
                typeOneHot[11] = 1
            if 'History' in row['genres']:
                typeOneHot[12] = 1
            if 'Horror' in row['genres']:
                typeOneHot[13] = 1
            if 'Romance' in row['genres']:
                typeOneHot[14] = 1
            if 'Comedy' in row['genres']:
                typeOneHot[15] = 1
            if 'Sport' in row['genres']:
                typeOneHot[16] = 1
            if 'News' in row['genres']:
                typeOneHot[17] = 1
            if 'Family' in row['genres']:
                typeOneHot[18] = 1
            if 'Action' in row['genres']:
                typeOneHot[19] = 1
            if 'War' in row['genres']:
                typeOneHot[20] = 1
            if 'Drama' in row['genres']:
                typeOneHot[21] = 1
            self.movies_15.loc[i,'month'] = row['release_time'][5:]
            self.movies_15.loc[i,'typeOneHot'] = ','.join(str(i) for i in typeOneHot)
            index = index + 1
            pass
        print(index)
        if os.path.exists("complate.xlsx"):
            os.remove("complate.xlsx")

        writer = pd.ExcelWriter('complate.xlsx')
        self.movies_15.to_excel(writer,sheet_name='Sheet1')
        writer.save()
        pass


    def start(self):
        self.differDirectorRevenue()
        #self.diiferActorRevenue()
        #self.redExcel()
        pass


app = PredictFormateDate()
app.start()


#390个导演


#导演667
