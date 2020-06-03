#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/5/24 21:05
# @Author  : luozujian
from imdbTvSpider import ImdbTvSpider
from detailTvProducer import DetailTvProducer
from detailTvConsumer import DetailTvConsumer
import threading,time

class TvSpider:

    def __init__(self):
        pass

    def startImDbSpider(self):
        ImdbTvSpider().start()

    def startDetailTvProducer(self):
        DetailTvProducer().start()

    def start(self):
        thead_one = threading.Thread(target=self.startImDbSpider)
        #thead_one.start()

        thead_two = threading.Thread(target=self.startDetailTvProducer)
        thead_two.start()


        for i in range(5):
            DetailTvConsumer().start()

if __name__ == "__main__":
    app = TvSpider()
    app.start()