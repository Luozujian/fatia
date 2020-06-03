#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/31 11:06
# @Author  : luozujian
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from scipy import spatial
from sqlalchemy import create_engine
import os
import pymysql
from tvStuctureDate import TvStuctureDate
from ReadData import DBConnection
from votePca import VotePca
from matlabtest import MatlabTest

class TvPredict():
    def __init__(self):
        self.conn , self.cur = DBConnection.getDBConnection()
        pass


    #获取改导演所导过的电视剧的平均评分
    def averageVoteForDirector(self,director):
        director = director.strip()
        sql = 'select * from fatia_tv_copy1 where director = "%s" limit 0 , 100'  % director
        self.cur.execute(sql)
        diVoteAv = 0.0
        tvDates = self.cur.fetchall()
        if tvDates is None or len(tvDates) == 0:
            #默认是7.5分
            diVoteAv = 7.5
            return diVoteAv
        for item in tvDates:
            diVoteAv += float(item[2])
        return diVoteAv/len(tvDates)

    #获取演员的平均评分
    def avgVoteActors(self,actors):
        actorsVote = 0.0
        actors = actors.split(",")
        for actor in actors:
            tempSum = 0.0
            sql = 'select * from fatia_tv_copy1 where actors like "%s%s%s" limit 0,100' % ("%",actor.strip(),"%")
            print(sql)
            self.cur.execute(sql)
            tvDates = self.cur.fetchall()
            if tvDates is None or len(tvDates) == 0:
                tempSum = 7.5
                actorsVote += tempSum
                continue

            for item in tvDates:
                print(item)
                tempSum += float(item[2])

            tempSum = tempSum/len(tvDates)
            actorsVote += tempSum
        actorsVote/=len(actors)
        return actorsVote

    def predict(self,date):
        director = date["director"]
        actors = date["actors"]

        diVoteAv = self.averageVoteForDirector(director)
        acVoteAv = self.avgVoteActors(actors)

        totalVote = diVoteAv*0.6 + acVoteAv*0.4
        date['vote_average'] = totalVote
        stuctApp = TvStuctureDate()
        stuctApp.predict(date)
        stuctApp.start()
        VotePca().start()

        vote = 0.0
        voteResult = MatlabTest().getTvResult()
        try:
            voteResult = voteResult[0]
            for item in voteResult:
                vote += item
            vote = vote/len(voteResult)
        except Exception:
            vote = voteResult

        return vote

if __name__ == "__main__":
    date = {
        "id": "-1",
        "title": "The Last Kingdom",
        "types": "Action,Drama,History",
        "time": "132",
        "director": "Michael Dante DiMartino",
        "actors": "Alexander Dreymon, Eliza Butterworth, Arnas Fedaravicius",
        "area": "USA",
        "vote_average": "8.7",
        "hot": "10000"
    }
    app = TvPredict()
    app.predict(date)