#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/21 22:07
# @Author  : luozujian
import pandas as pd
import numpy as np
from numpy import *
import scipy.io as sio
from matlabtest import MatlabTest
from votePCA import VotePca
from revenuePca import RevenuePca
#如果演员或者导演不在数据集里面就给它设置成3

class Predict:

    def __init__(self):
        self.movie = pd.read_excel("D:\\pythonproject\\bishe\\app\\pretreatment\\date\\complate_vote.xlsx",sheet_name="Sheet1")
        self.movie = self.movie.head(100)
        self.movie1 = pd.read_excel("D:\\pythonproject\\bishe\\app\\pretreatment\\date\\complate_revenue.xlsx",sheet_name="Sheet1")
        self.movie1 = self.movie1.head(100)
        self.actorsLenth = 0
        pass

    #进行onehot编码
    def getOneHot(self,date):
        typeOneHot = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        if 'Sci-Fi' in date:
            typeOneHot[0] = 1
        if 'Biography' in date:
            typeOneHot[1] = 1
        if 'Mystery' in date:
            typeOneHot[2] = 1
        if 'Western' in date:
            typeOneHot[3] = 1
        if 'Musical' in date:
            typeOneHot[4] = 1
        if 'Thriller' in date:
            typeOneHot[5] = 1
        if 'Music' in date:
            typeOneHot[6] = 1
        if 'Crime' in date:
            typeOneHot[7] = 1
        if 'Animation' in date:
            typeOneHot[8] = 1
        if 'Adult' in date:
            typeOneHot[9] = 1
        if 'Adventure' in date:
            typeOneHot[10] = 1
        if 'Fantasy' in date:
            typeOneHot[11] = 1
        if 'History' in date:
            typeOneHot[12] = 1
        if 'Horror' in date:
            typeOneHot[13] = 1
        if 'Romance' in date:
            typeOneHot[14] = 1
        if 'Comedy' in date:
            typeOneHot[15] = 1
        if 'Sport' in date:
            typeOneHot[16] = 1
        if 'News' in date:
            typeOneHot[17] = 1
        if 'Family' in date:
            typeOneHot[18] = 1
        if 'Action' in date:
            typeOneHot[19] = 1
        if 'War' in date:
            typeOneHot[20] = 1
        if 'Drama' in date:
            typeOneHot[21] = 1



        return ','.join('%s'% item for item in typeOneHot)



    #将28纬度降到16纬

    # def transform(self):
    #     pca = pd.read_excel("D:\\pythonproject\\bishe\\app\\pretreatment\\coefficient.xlsx", sheet_name="sheet1",header=None)
    #     stList = pd.read_excel("D:\\pythonproject\\bishe\\app\\pretreatment\\stList.xlsx", sheet_name="sheet1",header=None)
    #     stList = stList.values
    #     stList = mat(stList)
    #
    #     stList = np.transpose(stList)
    #     stList = np.array(stList)
    #     index = 0
    #
    #     for i ,row in self.predictMovie.iteritems():
    #         self.predictMovie[i] = self.predictMovie[i].astype(float)
    #         self.predictMovie[i] = self.predictMovie[i].map(lambda x: round((x - stList[0][index])/stList[1][index] ,10))
    #         index = index + 1
    #
    #     pca = np.array(pca)
    #     predictMovie = np.array(self.predictMovie)
    #     movieList = list()
    #     for i in range(len(predictMovie)):
    #         matrix = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    #         for j in range(16):
    #             temp = 0
    #             for k in range(28):
    #                 temp = temp + (float(predictMovie[i][k]) * float(pca[j][k]))
    #             matrix[j] = temp
    #         movieList.append(matrix)
    #     return movieList





    def predictMovie(self,date):
        #预算,评分,票房,时长,年,月,导演，演员等级
        actorLever = list()
        direcotrLever = 2

        self.actors = date[4].split(',')
        self.actorsLenth = len(self.actors)
        for i in range(self.actorsLenth):
            flagActor = False
            flagDirector = False
            newActor = self.actors[i]
            index = 1
            for j ,row in self.movie.iterrows():
                if row['actor'] == newActor and flagActor == False:
                    actorLever.append(row['actorlever'])
                    flagActor = True
                if row['director'] == date[3] and flagDirector == False:
                    direcotrLever = row['directorlever']
                    flagDirector = True
                index = index + 1
            if flagActor == False:
                actorLever.append(2)

        print(actorLever,direcotrLever)

        year = "2011"
        month = "09"
        self.predictMovie = pd.DataFrame(columns=['Unnamed: 0','Unnamed: 0.1','title','release_time','genres','actor','budget','revenue','director','vote_average','time','year','directorlever','actorlever','typeOneHot','month'])

        oneHot = self.getOneHot("Action,Drama,Thriller")

        for i in range(self.actorsLenth):
            dateTemp = {"revenue":"60000000","release_time":"2011-09-01","typeOneHot":oneHot,"budget":"60000000","time":"106","year":year,"month":month,"directorlever":direcotrLever,"actorlever":actorLever[i]}
            self.predictMovie = self.predictMovie.append(dateTemp,ignore_index=True)

        self.movie = self.movie.append(self.predictMovie,ignore_index=True)
        self.movie1 = self.movie1.append(self.predictMovie,ignore_index=True)


        writer = pd.ExcelWriter('D:\\pythonproject\\bishe\\app\\pretreatment\\date\\complate_vote_temp.xlsx')
        self.movie.to_excel(writer, sheet_name='Sheet1')
        writer.save()

        writer = pd.ExcelWriter('D:\\pythonproject\\bishe\\app\\pretreatment\\date\\complate_revenue_temp.xlsx')
        self.movie1.to_excel(writer, sheet_name='Sheet1')
        writer.save()

        #开启pca
        VotePca().start()
        RevenuePca().start()

        voteResult = MatlabTest().getVoteResult()
        revenueResult = MatlabTest().getRevenueResult()

        vote = 0.0
        revenue = 0.0

        try:
            voteResult = voteResult[0]
            for item in voteResult:
                vote += item
            vote = vote/len(voteResult)
        except Exception:
            vote = voteResult

        try:
            revenueResult = revenueResult[0]
            for item in revenueResult:
                revenue += item
            revenue = revenue/len(revenueResult)
        except Exception:
            revenue = revenueResult

        print(vote,revenue)
        return revenue,vote




if __name__ == "__main__":
    date = ["","","","Steven Soderbergh","Matt Damon,Kate Winslet,Jude Law"]

    app = Predict()
    app.predictMovie(date)