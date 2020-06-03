#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/21 22:30
# @Author  : luozujian

from flask import Flask
from views import admin

app  =  Flask(__name__)
app.register_blueprint(admin, url_prefix='/admin')




if  __name__  ==  '__main__':

    app.run(host='0.0.0.0',  debug=True)