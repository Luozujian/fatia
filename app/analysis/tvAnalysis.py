#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/25 10:47
# @Author  : luozujian
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from scipy import spatial
from sqlalchemy import create_engine
import os



class TvAnalysis:
    def __init__(self):
        plt.style.use('ggplot')
        pd.set_option('display.max_columns', 1000)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', 1000)
        warnings.filterwarnings(action='ignore')
        engin = create_engine("mysql+pymysql://root:luozujian@localhost:3306/fatia", encoding='utf-8')
        sql = 'select * from fatia_tv_copy1'
        self.movies = pd.read_sql_query(sql, con=engin)
        self.baseImagePath = 'D:\\pythonproject\\bishe\\app\\flask\\myapp\\static\\img\\'
        self.handleDate()
        pass

    #对空的数据进行清洗
    def handleDate(self):
        for index,row in self.movies.iteritems():

            directorSerie = self.movies[index]
            directorSerie[directorSerie == ""] = np.nan
            self.movies[index] = directorSerie

        self.movies = self.movies.dropna(axis = 0,how= 'any')
        self.movies['year'] = self.movies.apply(lambda x: x['release_time'][0:4], axis=1)
        self.movies['year'] = self.movies['year'].astype(int)
        self.movies['vote_average'] = self.movies['vote_average'].astype(float)
        pass



    # 评分的总体趋势
    def voteTrand(self):
        vote_by_year = self.movies.groupby('year').vote_average.mean()
        vote_by_year.plot(title='Evolution of Average Vote')
        imgPath = self.baseImagePath + 'tv_voteTrand.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)


    def countN(self,column):
        count = dict()
        for row in column:
            types = row.split(',')
            for type in types:
                type = type.strip()
                if type in count:
                    count[type] += 1
                else:
                    count[type] = 1
        return count

    #不同类型电视剧的评分
    def voteOfKindsTv(self):
        type = pd.Series(self.countN(self.movies.types)).sort_values()
        genres = type
        moviews_by_genres = pd.DataFrame(0, index=genres.index, columns=['vote'])
        for i,v in self.movies.types.items():
            datas = v.split(',')
            for g in datas:
                moviews_by_genres.loc[g, 'vote'] += self.movies.vote_average[i]
        moviews_by_genres = moviews_by_genres.div(genres.values, axis=0)
        moviews_by_genres.sort_values('vote').vote.plot(kind='barh', title='Vote in Different Genres')
        imgPath = self.baseImagePath + 'tv_voteOfKindsTv.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)


    # 导演的平均评分
    def voteOfDirector(self):
        vote_of_director = self.movies.groupby('director').vote_average.mean()
        vote_of_director.hist()
        imgPath = self.baseImagePath + 'tv_voteOfDirector.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)
        # plt.show()


    # 评分最高的导演top10
    def top10voteDirector(self):
        vote_of_director = self.movies.groupby('director').vote_average.mean()
        vote_of_director.sort_values().tail(10).plot(kind='barh', title="Director with Top vote", figsize=(9, 4))
        # plt.show()
        imgPath = self.baseImagePath + 'tv_top10voteDirector.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)

    # 演员的评分分布
    def voteOfActors(self):
        actors = pd.Series(self.countN(self.movies.actors)).sort_values()
        movies_by_actors = pd.DataFrame(0, index=actors.index, columns=['vote'])
        r4 = [0.4, 0.3, 0.2, 0.1]  # 如果有4位主演，按此加权，一下类似
        r3 = [0.4, 0.3, 0.3]
        r2 = [0.6, 0.4]
        r1 = [1]
        r = [r1, r2, r3, r4]
        for i, v in self.movies.actors.items():
            actorlist = v.split(',')
            size = 0
            if len(actorlist) >= 4:
                size = 4
            else:
                size = len(actorlist)
            for j in range(len(actorlist)):
                if j > 4:
                    continue

                actorlist[j] = actorlist[j].strip()
                movies_by_actors.loc[actorlist[j], 'vote'] += self.movies.vote_average[i]

        movies_by_actors = movies_by_actors.div(actors.values, axis=0)
        movies_by_actors.vote.hist()
        imgPath = self.baseImagePath + 'tv_voteOfActors.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)
        # plt.show()

    def start(self):

        #评分的变化的总体趋势
        #self.voteTrand()

        #不同类型电视剧的评分
        #self.voteOfKindsTv()

        #导演的平均评分
        #self.voteOfDirector()

        #评分最高的导演top10
        #self.top10voteDirector()

        #演员评分的分布
        #self.voteOfActors()

        pass

        #评分最高的演员




if __name__ == "__main__":
    app = TvAnalysis()
    app.start()