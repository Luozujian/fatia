#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/21 20:55
# @Author  : luozujian
import os
import pymysql
class DBConnection:

    #获取文件的绝对路径，用于读取data下面的数据
    @classmethod
    def getDBConnection(self):
        conn = pymysql.connect(host='127.0.0.1', user='root', password='luozujian', db='fatia', charset='utf8')
        cur = conn.cursor()
        return conn,cur

