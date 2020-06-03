#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/11 20:12
# @Author  : luozujian
from detailMovieConsumer import DetailConsumer
from detailMovieProducer import DetailProducer
from imdbMovieSpider import ImdbSpider
import threading,time

class StartSpider:

    def __int__(self):
        print(1)
        pass

    def imdbSpiderStart(self):
        ImdbSpider().start()

    def detailProducerStart(self):
        DetailProducer().start()

    def start(self):
        thead_one = threading.Thread(target=self.imdbSpiderStart)
        thead_one.start()

        thead_two = threading.Thread(target=self.detailProducerStart)
        thead_two.start()

        for i in range(5):
            DetailConsumer().start()
        pass

