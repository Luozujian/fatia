#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/10 14:51
# @Author  : luozujian
from numpy import *
import numpy as np
import scipy.io as sio
import pandas as pd
import xlwt
from sklearn.datasets import make_blobs
from sklearn.decomposition import PCA
import matplotlib
import matplotlib.pyplot as plt
class VotePca:

    def __init__(self):
        self.movie2 = pd.read_excel("D:\\pythonproject\\bishe\\app\\pretreatment\\date\\complate_vote_temp.xlsx",sheet_name = "Sheet1")
        #self.movie2 = self.movie2.head(50)
        pass




        # fig = plt.figure()
        # ax = fig.add_subplot(111)
        # ax.scatter(dataMat[:, 0].flatten().A[0], dataMat[:, 1].flatten().A[0], marker='^', s=90)
        # ax.scatter(reconMat[:, 0].flatten().A[0], reconMat[:, 1].flatten().A[0], marker='o', s=50, c='red')
        # plt.show()

    def save(self,data, path):
        f = xlwt.Workbook()  # 创建工作簿
        sheet1 = f.add_sheet(u'sheet1', cell_overwrite_ok=True)  # 创建sheet
        [h, l] = data.shape  # h为行数，l为列数
        for i in range(h):
            for j in range(l):
                sheet1.write(i, j, data[i, j])
        f.save(path)


    def start(self):
        self.movie2['month'] = pd.Series(self.movie2['release_time']).str[5:7].astype(float)
        self.movie2['Sci-Fi'] = None
        self.movie2['Biography'] = None
        self.movie2['Mystery'] = None
        self.movie2['Western'] = None
        self.movie2['Musical'] = None
        self.movie2['Music'] = None
        self.movie2['Animation'] = None
        self.movie2['Adult'] = None
        self.movie2['Adventure'] = None
        self.movie2['Fantasy'] = None
        self.movie2['History'] = None
        self.movie2['Horror'] = None
        self.movie2['Romance'] = None
        self.movie2['Comedy'] = None
        self.movie2['Sport'] = None
        self.movie2['News'] = None
        self.movie2['Family'] = None
        self.movie2['Action'] = None
        self.movie2['War'] = None
        self.movie2['Drama'] = None

        array = ['Sci-Fi', 'Biography', 'Mystery', 'Western', 'Musical', 'Thriller', 'Music', 'Crime', 'Animation', 'Adult', 'Adventure', 'Fantasy', 'History', 'Horror', 'Romance', 'Comedy', 'Sport', 'News', 'Family', 'Action', 'War', 'Drama']
        index = 1
        for i ,row in self.movie2.iterrows():
            print(index)
            oneHot = row['typeOneHot'].split(',')
            for j in range(len(oneHot)):
                if int(oneHot[j]) == 1:
                    self.movie2.loc[i,array[j]] = 1
                else:
                    self.movie2.loc[i,array[j]] = 0
            index = index + 1


        #stList = list()

        self.y = pd.Series(self.movie2['vote_average'])
        self.x = self.movie2.drop(['Unnamed: 0','Unnamed: 0.1','Unnamed: 0.1.1','title','release_time','genres','actor','director','revenue','typeOneHot','vote_average'],axis=1)

        # 求每一列的平均值和方差
        # for index, row in self.x.iteritems():
        #     st = list()
        #     st.append(self.x[index].mean())
        #     st.append(self.x[index].var())
        #
        #     stList.append(st)
        #     print("lala",index)  #输出列名


        #每列的方差，标准差
        #stList = np.matrix(stList)
        #stList = np.transpose(stList)
        #$self.save(stList, "stList.xlsx")

        pca = PCA(n_components=16)
        pca.fit(self.x)

        #print(pca.explained_variance_ratio_)
        #print(pca.explained_variance_)
        #print(pca.n_components_)

        # temp = pca.components_
        # temp = mat(temp)
        # self.save(temp,"coefficient.xlsx")


        x_new = pca.transform(self.x)
        y_new = self.y.values

        sio.savemat('movie.mat',{'NIR':x_new,'octane':y_new})

        #self.x = mat(self.x)
        #self.test(self.x)
        # writer = pd.ExcelWriter('lala.xlsx')
        # self.x.to_excel(writer, float_format='%.5f')
        # writer.save()
        pass


if __name__ == "__main__":
    app = VotePca()
    #app.test()
    app.start()





# PCA分析评分与演员，导演，时长，种类，上映年份，上映月份，投资,类型之间的关系



