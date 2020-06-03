#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/26 10:50
# @Author  : luozujian
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import seaborn as sns
from scipy import spatial
from sqlalchemy import create_engine
from sklearn.decomposition import PCA
import os
import scipy.io as sio


class VotePca:

    def __init__(self):
        engin = create_engine("mysql+pymysql://root:luozujian@localhost:3306/fatia", encoding='utf-8')
        sql = 'select * from fatia_tv_stucture'
        self.tvStucture = pd.read_sql_query(sql,con=engin)
        pass

    def tranceOneHot(self):
        self.tvStucture['Action'] = None
        self.tvStucture['Adventure'] = None
        self.tvStucture['Animation'] = None
        self.tvStucture['Biography'] = None
        self.tvStucture['Comedy'] = None
        self.tvStucture['Crime'] = None
        self.tvStucture['Documentary'] = None
        self.tvStucture['Drama'] = None
        self.tvStucture['Family'] = None
        self.tvStucture['Fantasy'] = None
        self.tvStucture['Game Show'] = None
        self.tvStucture['History'] = None
        self.tvStucture['Horror'] = None
        self.tvStucture['Music'] = None
        self.tvStucture['Musical'] = None
        self.tvStucture['Mystery'] = None
        self.tvStucture['News'] = None
        self.tvStucture['Reality-TV'] = None
        self.tvStucture['Romance'] = None
        self.tvStucture['Sci-Fi'] = None
        self.tvStucture['Sport'] = None
        self.tvStucture['Superhero'] = None
        self.tvStucture['Talk Show'] = None
        self.tvStucture['Thriller'] = None
        self.tvStucture['War'] = None
        self.tvStucture['Western'] = None

        array = ['Action', 'Adventure', 'Animation', 'Biography', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family', 'Fantasy', 'Game Show', 'History', 'Horror', 'Music', 'Musical', 'Mystery', 'News', 'Reality-TV', 'Romance', 'Sci-Fi', 'Sport','Superhero','Talk Show','Thriller','War','Western']
        index = 1
        for i, row in self.tvStucture.iterrows():
            print(index)
            oneHot = row['onehot'].split(',')
            for j in range(len(oneHot)):
                if int(oneHot[j]) == 1:
                    self.tvStucture.loc[i, array[j]] = 1
                else:
                    self.tvStucture.loc[i, array[j]] = 0
            index = index + 1
        self.tvStucture = self.tvStucture.drop(['onehot'],axis = 1)
        pass



    def start(self):

        self.tranceOneHot()
        self.tvStucture['hot'] = self.tvStucture.apply(lambda x: x['hot'].replace(",",""), axis=1)


        self.y = pd.Series(self.tvStucture['vote_average']).astype(float)
        self.x = self.tvStucture.drop(['id','tv_id','vote_average'],axis=1)




        pca = PCA(n_components=16)
        pca.fit(self.x)

        x_new = pca.transform(self.x)
        y_new = self.y.values

        sio.savemat('D:\\pythonproject\\bishe\\app\\flask\\vote_tv.mat', {'NIR': x_new, 'octane': y_new})


        pass

if __name__ == "__main__":
    app = VotePca()
    app.start()


