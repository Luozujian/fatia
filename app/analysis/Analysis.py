#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/19 15:16
# @Author  : luozujian
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from scipy import spatial
from sqlalchemy import create_engine
import os


class Analysis:
    def __init__(self):
        plt.style.use('ggplot')
        pd.set_option('display.max_columns', 1000)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', 1000)
        warnings.filterwarnings(action='ignore')
        engin = create_engine("mysql+pymysql://root:luozujian@localhost:3306/fatia",encoding='utf-8')
        sql = 'select * from fatia_3'
        self.movies = pd.read_sql_query(sql,con=engin)
        self.movies = self.movies.drop_duplicates(['title'],keep='first')
        self.movies = self.movies.drop(['id','area','market','flag','producer','audience_type','heat','url'],axis=1)
        self.movies.year = self.movies.year.astype('int')
        self.movies.budget = self.movies.budget.astype('float')
        self.movies.revenue = self.movies.revenue.astype('float')
        self.movies.vote_average = self.movies.vote_average.astype('float')
        self.movies_15 = self.movies[(self.movies.year >= 2005) & (self.movies.year < 2020) & (self.movies.budget*self.movies.revenue*self.movies.vote_average!=0)]
        self.movies_noani = self.movies_15[~self.movies_15.genres.str.contains('Animation',regex=False)].reset_index(drop=True)

        self.baseImagePath = 'D:\\pythonproject\\bishe\\app\\flask\\myapp\\static\\img\\'
        pass

    # 获取文件的绝对路径，用于读取data下面的数据
    def getPath(self, relativePaths):
        PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
        path = os.path.join(PROJECT_ROOT, relativePaths)
        return path


    #每年的电影数目
    def moviesNumber(self):
        self.movies[self.movies.year >= 2005].groupby('year').size().plot(kind='bar',color=['r','g','b', 'c', 'm', 'y'])
        imgPath = self.baseImagePath+'moviesNumber.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)

        plt.savefig(imgPath)
        #plt.show()


    #电影票房的top10
    def top10ofMovies(self):
        data = self.movies_15.sort_values('revenue',ascending=False)[['title','revenue','budget','genres']][0:10]
        data.plot(kind='barh')
        #plt.show()

    #票房，预算和投资回报率
    def revenueAndBugetAndRateOfReturn(self):
        b_r = self.movies_15.groupby('year')['budget','revenue'].sum()
        b_r['ROI'] = (b_r.revenue - b_r.budget) / b_r.budget
        # 画图
        fig,axes = plt.subplots(2,1,figsize=(6,6))
        b_r.iloc[:,0:2].plot(kind='bar',ax=axes[0],title='Budget and Revenue')
        axes[0].set_ylabel('Dollar')
        b_r.ROI.plot(ax=axes[1],title='Evolution of ROI')
        fig.tight_layout()
        imgPath = self.baseImagePath+'revenueAndBugetAndRateOfReturn.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)

        plt.savefig(imgPath)

        #plt.show()

    #评分top10
    def top10Vote(self):
        data = self.movies_15.sort_values('vote_average' , ascending=False)[['title','vote_average','genres']][0:10]
        print(data)

    #评分的总体趋势
    def voteTrand(self):
        vote_by_year = self.movies_15.groupby('year').vote_average.mean()
        vote_by_year.plot(title = 'Evolution of Average Vote')
        imgPath = self.baseImagePath + 'voteTrand.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)
        #plt.show()

    # 票房和评分影响的因数
    def factors(self):
        sns.heatmap(self.movies_15.corr(),annot=True,vmax=1,vmin=0,square=True,cmap='Blues')
        imgPath = self.baseImagePath + 'factors.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)
        #plt.show()
        pass

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
    # 影片类型的变化趋势
    def typeOfMovieTrend(self):
        genres = pd.Series(self.countN(self.movies_15.genres)).sort_values()
        genres_avg = genres/len(self.movies_15)
        genres_avg.plot(kind='barh',title='Frequency of Genres')
        imgPath = self.baseImagePath + 'typeOfMovieTrend.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)
        #plt.show()

    #选取前9中类型，观察它们在15年间每年的数目与当年影片总数之比的变化
    def kindsOfMovieRate(self):
        genres = pd.Series(self.countN(self.movies_15.genres)).sort_values()
        genres = genres.drop(['Adult','News'])
        genres_by_year = self.movies_15.groupby('year').genres.sum()
        genres_count = pd.DataFrame([],index=genres_by_year.index,columns=genres.index[0:17])
        for g in genres_count.columns:
            for y in genres_count.index:
                genres_count.loc[y,g] = genres_by_year[y].count(g)/len(genres_by_year[y])
        genres_count.plot(figsize = (10,6),title = "Evolution of moview in 9 Genres")
        #plt.show()

    #不同类型影片的票房
    def revenueOfKindsMovie(self):
        genres = pd.Series(self.countN(self.movies_15.genres)).sort_values()
        genres = genres.drop(['Adult', 'News'])
        moviews_by_genres = pd.DataFrame(0, index=genres.index, columns=['revenue', 'budget', 'vote'])
        for i,v in self.movies_15.genres.items():
            datas = v.split(',')
            for g in datas:
                if g is 'Adult' or g is 'News':
                    continue
                if 'dul' in g or 'News' in g:
                    continue

                moviews_by_genres.loc[g, 'revenue'] += self.movies_15.revenue[i]
                moviews_by_genres.loc[g, 'budget'] += self.movies_15.budget[i]
                moviews_by_genres.loc[g, 'vote'] += self.movies_15.vote_average[i]

        moviews_by_genres = moviews_by_genres.div(genres.values, axis=0)
        moviews_by_genres['ROI'] = (moviews_by_genres.revenue-moviews_by_genres.budget)/moviews_by_genres.budget

        #画图
        fig,axes = plt.subplots(2,1,figsize = (8,8))
        moviews_by_genres.sort_values('revenue',ascending=False)[['revenue','budget']].plot(ax=axes[0],kind='bar',title='Average Revenue and Budget in Different Genres')
        moviews_by_genres.sort_values('revenue',ascending=False)['ROI'].plot(ax=axes[1],kind='bar',title='ROI in Different Genres')
        fig.tight_layout()

        imgPath = self.baseImagePath + 'revenueOfKindsMovie.png'
        print(imgPath)
        if os.path.exists(imgPath):
            os.remove(imgPath)

        plt.savefig(imgPath)
        #plt.show()

    #对比一下不同类型的影片是否比单纯的科幻片或单纯的剧情片有更高的投资回报率
    def differTypeImpactOfRevenue(self):

        genres_c = pd.Series()
        movies_dra_sci = self.movies_15[self.movies_15.genres.str.contains('Action',regex=False) & self.movies_15.genres.str.contains('Sci-Fi',regex=False)]
        genres_c['A'] = (movies_dra_sci.revenue.sum()-movies_dra_sci.budget.sum())/movies_dra_sci.budget.sum()

        movies_dra = self.movies_15[self.movies_15.genres.str.contains('Action',regex=False) & ~self.movies_15.genres.str.contains('Sci-Fi',regex=False)]
        genres_c['B'] = (movies_dra.revenue.sum()-movies_dra.budget.sum())/movies_dra.budget.sum()

        movies_sci = self.movies_15[~self.movies_15.genres.str.contains('Action',regex=False) & self.movies_15.genres.str.contains('Sci-Fi',regex=False)]
        genres_c['C'] = (movies_sci.revenue.sum()-movies_sci.budget.sum())/movies_sci.budget.sum()

        genres_c.plot(kind = 'barh',title='ROI')

        #plt.show()

    #不同类型电影的评分
    def voteOfKindsMovie(self):
        genres = pd.Series(self.countN(self.movies_15.genres)).sort_values()
        genres = genres.drop(['Adult', 'News'])
        moviews_by_genres = pd.DataFrame(0, index=genres.index, columns=['revenue', 'budget', 'vote'])
        for i,v in self.movies_15.genres.items():
            datas = v.split(',')
            for g in datas:
                if g is 'Adult' or g is 'News':
                    continue
                if 'dul' in g or 'News' in g:
                    continue

                moviews_by_genres.loc[g, 'revenue'] += self.movies_15.revenue[i]
                moviews_by_genres.loc[g, 'budget'] += self.movies_15.budget[i]
                moviews_by_genres.loc[g, 'vote'] += self.movies_15.vote_average[i]
        moviews_by_genres = moviews_by_genres.div(genres.values, axis=0)
        moviews_by_genres['ROI'] = (moviews_by_genres.revenue - moviews_by_genres.budget) / moviews_by_genres.budget
        moviews_by_genres.sort_values('vote').vote.plot(kind='barh', title='Vote in Different Genres')
        imgPath = self.baseImagePath + 'voteOfKindsMovie.png'
        if os.path.exists(imgPath):
            os.remove(imgPath)
        plt.savefig(imgPath)

        #plt.show()


    #导演的票房分布
    def revenueOfDirector(self):
        revenue_of_director = self.movies_15.groupby('director').revenue.mean()
        revenue_of_director.hist(bins=100,figsize = (9,4))
        #plt.show()


    #票房最高的导演Top10
    def top10RevenueOfDirector(self):
        revenue_of_director = self.movies_15.groupby('director').revenue.mean()
        revenue_of_director.sort_values().tail(10).plot(kind='barh',title="Director with top Revenue",figsize=(10,4))
        #plt.show()


    #科幻片导演的票房前10
    def top10DirectorOfSciFic(self):
        revenue_director_genres = self.movies_15[self.movies_15.genres.str.contains('Sci-Fi',regex = False)].groupby('director').revenue.mean()
        revenue_director_genres.sort_values().tail(10).plot(kind='barh',title='Director of SF Movies with Top Revenue',figsize=(10,4))
        #plt.show()

    #导演的平均评分
    def voteOfDirector(self):
        vote_of_director = self.movies_15.groupby('director').vote_average.mean()
        vote_of_director.hist()
        #plt.show()

    #评分最高的导演top10
    def top10voteDirector(self):
        vote_of_director = self.movies_15.groupby('director').vote_average.mean()
        vote_of_director.sort_values().tail(10).plot(kind='barh',title="Director with Top vote",figsize=(9,4))
        #plt.show()

    #我们只考虑前四位主演，每位主演对票房的贡献按下面的列表来计算
    def actorsRevenue(self):
        actors = pd.Series(self.countN(self.movies_noani.actors)).sort_values()
        movies_by_actors = pd.DataFrame(0,index = actors.index, columns=['revenue','vote'])
        r4 = [0.4,0.3,0.2,0.1] #如果有4位主演，按此加权，一下类似
        r3 = [0.4,0.3,0.3]
        r2 = [0.6,0.4]
        r1 = [1]
        r = [r1,r2,r3,r4]
        for i, v in self.movies_noani.actors.items():
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
                print(actorlist[j])
                movies_by_actors.loc[actorlist[j],'revenue'] += self.movies_noani.revenue[i]*r[size-1][j]
                movies_by_actors.loc[actorlist[j],'vote'] += self.movies_noani.vote_average[i]

        movies_by_actors = movies_by_actors.div(actors.values,axis = 0)
        movies_by_actors.revenue.hist(bins = 100)
        #plt.show()


    #票房最高演员的top10
    def top10OfActors(self):
        actors = pd.Series(self.countN(self.movies_noani.actors)).sort_values()
        movies_by_actors = pd.DataFrame(0, index=actors.index, columns=['revenue', 'vote'])
        r4 = [0.4, 0.3, 0.2, 0.1]  # 如果有4位主演，按此加权，一下类似
        r3 = [0.4, 0.3, 0.3]
        r2 = [0.6, 0.4]
        r1 = [1]
        r = [r1, r2, r3, r4]
        for i, v in self.movies_noani.actors.items():
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
                movies_by_actors.loc[actorlist[j], 'revenue'] += self.movies_noani.revenue[i] * r[size - 1][j]
                movies_by_actors.loc[actorlist[j], 'vote'] += self.movies_noani.vote_average[i]

        movies_by_actors = movies_by_actors.div(actors.values, axis=0)
        movies_by_actors.revenue.sort_values().tail(10).plot(kind='barh',figsize = (12,4))
        #plt.show()

    #演员的评分分布
    def voteOfActors(self):
        actors = pd.Series(self.countN(self.movies_noani.actors)).sort_values()
        movies_by_actors = pd.DataFrame(0, index=actors.index, columns=['revenue', 'vote'])
        r4 = [0.4, 0.3, 0.2, 0.1]  # 如果有4位主演，按此加权，一下类似
        r3 = [0.4, 0.3, 0.3]
        r2 = [0.6, 0.4]
        r1 = [1]
        r = [r1, r2, r3, r4]
        for i, v in self.movies_noani.actors.items():
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
                movies_by_actors.loc[actorlist[j], 'revenue'] += self.movies_noani.revenue[i] * r[size - 1][j]
                movies_by_actors.loc[actorlist[j], 'vote'] += self.movies_noani.vote_average[i]

        movies_by_actors = movies_by_actors.div(actors.values, axis=0)
        movies_by_actors.vote.hist()
        #plt.show()

    #档期分布
    def monthOfMovies(self):
        self.movies_15['month'] = pd.to_datetime(self.movies_15['release_time'],format='%Y-%m').apply(lambda x:x.month)
        self.movies_15.month.hist()
        #plt.show()
        pass
    # 计算每个月的影片的平均票房:
    def avgRevenue(self):
        self.movies_15['month'] = pd.to_datetime(self.movies_15['release_time'], format='%Y-%m').apply(
            lambda x: x.month)
        revenue_month = self.movies_15.groupby('month').revenue.sum()/self.movies_15.groupby('month').size()
        revenue_month.plot(kind='barh',title='Average Revence per Month')
        #plt.show()
        pass

    #评分与档期的关系
    def voteForMonth(self):
        self.movies_15['month'] = pd.to_datetime(self.movies_15['release_time'], format='%Y-%m').apply(
            lambda x: x.month)
        self.movies_15.boxplot(column='vote_average',by='month',figsize=(6,6))
        plt.show()


    #首先对于每部影片都构造二元数组表示类型，导演和主演
    def bianry(self,wordList0,wordlist):
        binary = []
        for word in wordList0.index:
            if word in wordlist:
                binary.append(1)
            else:
                binary.append(0)
        return binary


    # 预测
    def structuralBinary(self):
        genres = pd.Series(self.countN(self.movies_15.genres)).sort_values()
        self.movies_15['genres_bin'] = [self.bianry(genres, x) for x in self.movies_15.genres]
        directors = self.movies_15.groupby('director').size().sort_values(ascending=True)
        self.movies_15['director_bin'] = [self.bianry(directors,x) for x in self.movies_15.director]
        actors = pd.Series(self.countN(self.movies_15.actors)).sort_values(ascending=True)
        self.movies_15['actors_bin'] = [self.bianry(actors,x) for x in self.movies_15.actors]
        pass

    #计算两部影片的夹角
    def angle(self,movie1,movie2):
        dis_tot = 0
        iterlist = [[movie1.genres_bin,movie2.genres_bin],[movie1.director_bin,movie2.director_bin],[movie1.actors_bin,movie2.actors_bin]]
        for b1,b2 in iterlist:
            if(1 not in b1) or (1 not in b2):
                dis = 1
            else:
                dis = spatial.distance.cosine(b1,b2)
            dis_tot += dis
        return dis_tot

    # 预测
    def predictor(self,new_movie):
        self.structuralBinary()
        movie_bin = pd.Series()
        genres = pd.Series(self.countN(self.movies_15.genres)).sort_values()
        directors = self.movies_15.groupby('director').size().sort_values(ascending=True)
        actors = pd.Series(self.countN(self.movies_15.actors)).sort_values(ascending=True)
        movie_bin['genres_bin'] = self.bianry(genres,new_movie['genres'])
        movie_bin['director_bin'] = self.bianry(directors,new_movie['director'])
        movie_bin['actors_bin'] = self.bianry(actors,new_movie['actors'])

        vote = self.movies_15.copy()
        vote['angle'] = [self.angle(vote.iloc[i],movie_bin)for i in range(len(vote))]
        vote = vote.sort_values('angle')
        vote_avg = np.mean(vote.vote_average[0:5])
        return vote_avg

    def start(self):
        #self.moviesNumber()
        #self.top10ofMovies()
        #self.revenueAndBugetAndRateOfReturn()
        #self.top10Vote()
        #self.voteTrand()
        #self.factors()
        #self.typeOfMovieTrend()
        #self.kindsOfMovieRate()
        #self.revenueOfKindsMovie()
        #self.differTypeImpactOfRevenue()
        #self.voteOfKindsMovie()
        #self.revenueOfDirector()
        #self.top10RevenueOfDirector()
        #self.top10DirectorOfSciFic()
        #self.voteOfDirector()
        #self.top10voteDirector()
        #self.actorsRevenue()
        #self.top10OfActors()
        #self.voteOfActors()
        #self.monthOfMovies()
        #self.avgRevenue()
        #self.voteForMonth()
        #the_call_of_the_wild = {'genres':['Adventure','Drama','Family'],'director':['Chris Sanders'],'actors':['Harrison','Omar Sy','Cara Gee']}
        #print(self.predictor(the_call_of_the_wild))
        pass


