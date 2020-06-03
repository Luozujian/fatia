#!/usr/bin/env python
#-*- coding:utf-8 -*-
# @Time    : 2020/4/21 22:41
# @Author  : luozujian
from flask import Blueprint, render_template,request
from ReadData import DBConnection
from Analysis import Analysis
from StartSpider import StartSpider
from predict import Predict
from tvAnalysis import TvAnalysis
from tvSpider import TvSpider
from tvPredict import TvPredict
import threading
import json

admin = Blueprint('admin', __name__, template_folder='templates',static_url_path='/admin/static',static_folder='static')


@admin.route('/hello')
@admin.route('/hello')
def  hello():
    return  render_template('analysis.html')


@admin.route('/index',methods=['GET','POST'])
def  index():
    return  render_template('index.html')

@admin.route('/analysis',methods=['GET','POST'])
def analysis():
    return render_template('analysis.html')


@admin.route('/delete',methods=['POST'])
def deleteItem():
    itemId = request.form['id']
    print(itemId)
    conn,cur = DBConnection.getDBConnection()
    sql = 'delete from fatia_3 where id = %d'%int(itemId)
    print(sql)
    number = cur.execute(sql)
    conn.commit()
    return "success"


@admin.route('/table',methods=['GET','POST'])
def getTableDatas():
    currentPage = int(request.args.get("currentPage"))
    pageSize = int(request.args.get("pageSize"))
    content = request.args.get("searchContent")
    id = request.args.get("id")
    conn ,cur = DBConnection.getDBConnection()
    start = (currentPage - 1) * pageSize
    sql = "select * from fatia_3 where "
    if content!='':
        if id == '':
            sql = "select * from fatia_3 "
        elif int(id) == 1:
            sql += "title like '%" + content + "%'"
        elif int(id) == 2:
            sql += "director like  '%" + content + "%'"
        elif int(id) == 3:
            sql += "actors like '%" + content + "%'"
        elif int(id) == 4:
            sql += "release_time = '" + content + "'"
        else:
            sql = "select * from fatia_3"
    else:
        sql = "select * from fatia_3"
    sql += " limit %d,%d"%(start,pageSize)
    print(sql)
    cur.execute(sql)
    datas = cur.fetchall()
    return json.dumps(datas)



@admin.route('/table/total',methods=['GET','POST'])
def getTotalDatas():
    id = request.args.get("id")
    content = request.args.get("searchContent")
    sql = "select count(*) from fatia_3 WHERE "
    print(id == '')
    if content!='':
        if id == '':
            sql = "select count(*) from fatia_3"
        elif int(id) == 1:
            sql += "title like '%" + content + "%'"
        elif int(id) == 2:
            sql += "derector like  '%" + content + "%'"
        elif int(id) == 3:
            sql += "actors like '%" + content + "%'"
        elif int(id) == 4:
            sql += "release_time = '"+content+"'"
        else:
            sql = "select count(*) from fatia_3"
    else:
        sql = "select count(*) from fatia_3"
    conn, cur = DBConnection.getDBConnection()
    cur.execute(sql)
    return json.dumps(cur.fetchone())



@admin.route('/saveImg',methods=['GET','POST'])
def saveImg():

    name = request.args.get("name")
    print(name)
    if name == 'moviesNumber':
        Analysis().moviesNumber()
    if name == "revenueAndBugetAndRateOfReturn":
        Analysis().revenueAndBugetAndRateOfReturn()
    if name == "factors":
        Analysis().factors()
    if name == "typeOfMovieTrend":
        Analysis().typeOfMovieTrend()
    if name == "voteTrand":
        Analysis().voteTrand()
    if name == "revenueOfKindsMovie":
        #Analysis().revenueOfKindsMovie()  //速度过慢
        pass
    if name == "voteOfKindsMovie":
        #Analysis().voteOfKindsMovie()
        pass
    return "success"

@admin.route('/detail',methods=['GET','POST'])
def detail():
    return render_template('detail.html')

#开启爬虫
@admin.route('/startSpider',methods=['GET','POST'])
def startSpider():
    thead_one = threading.Thread(target=SpiderMain)
    thead_one.start()
    return "success"

def SpiderMain():
    print("lala")
    global isRunMovie
    isRunMovie = True
    StartSpider().start()


@admin.route('/getCountDateSpider',methods=['GET','POST'])
def getCountDateSpider():
    conn,cur = DBConnection.getDBConnection()
    sql = 'select count(*) from fatia_new'
    cur.execute(sql)
    count = cur.fetchone()
    print(count)
    count = count[0]
    return str(round((count/170000.0)*100,2))+'%'



@admin.route('/getIsRunSpirder',methods=['GET','POST'])
def getIsRunSpirder():
    if isRunMovie:
        return "1"
    else:
        return "0"
    pass

isRunMovie = False
isRunTv = False

@admin.route('/predict',methods=['GET','POST'])
def predict():
    return render_template('predict.html')

@admin.route('/forecast',methods=['GET','POST'])
def forecast():
    movieName = request.args.get("movieName")
    movieType = request.args.get("movieType")
    movieTime = request.args.get("movieTime")
    director = request.args.get("director")
    actors = request.args.get("actors")
    budget = request.args.get("budget")
    area = request.args.get("area")
    company = request.args.get("company")
    dates = [movieName,movieType,movieTime,director,actors,budget,area,company]
    revuenue , vote = Predict().predictMovie(dates)
    revuenue = round(revuenue,2)
    vote = round(vote,2)
    return str(revuenue)  + ',' + str(vote)


@admin.route('/recommond',methods=['GET','POST'])
def recommond():
    return render_template('recommond.html')

@admin.route('/recommonded',methods=['GET','POST'])
def recommonded():
    conn,cur = DBConnection.getDBConnection()
    type = request.args.get("type")
    type = type.lower()
    if type is None or len(type) == 0:
        sql = "select title,director,vote_average,url from fatia_3 order by vote_average desc limit 0,20"
    else:
        sql = 'select title,director,vote_average,url from fatia_3 where genres like "%s%s%s" order by vote_average desc limit 0,20' % ("%",type,"%")

    cur.execute(sql)
    return json.dumps(cur.fetchall())

@admin.route('/tv/dateshow',methods=['GET','POST'])
def dataShow():
    return render_template('tvDateShow.html')


@admin.route('/tv/table',methods=['GET','POST'])
def tvTable():
    currentPage = int(request.args.get("currentPage"))
    pageSize = int(request.args.get("pageSize"))
    content = request.args.get("searchContent")
    id = request.args.get("id")
    conn, cur = DBConnection.getDBConnection()
    start = (currentPage - 1) * pageSize
    sql = "select id,title,vote_average,types,director,url from fatia_tv_copy1 where "
    if content != '':
        if id == '':
            sql = "select id,title,vote_average,types,director,urlfrom fatia_tv_copy1 "
        elif int(id) == 1:
            sql += "title like '%" + content + "%'"
        elif int(id) == 2:
            sql += "director like  '%" + content + "%'"
        elif int(id) == 3:
            sql += "actors like '%" + content + "%'"
        else:
            sql = "select id,title,vote_average,types,director,url from fatia_tv_copy1"
    else:
        sql = "select id,title,vote_average,types,director,url from fatia_tv_copy1"
    sql += " limit %d,%d" % (start, pageSize)
    print(sql)
    cur.execute(sql)
    datas = cur.fetchall()
    return json.dumps(datas)

@admin.route('/tv/analysis',methods=['GET','POST'])
def tvAnalysis():
    return render_template('tvAnalysis.html')

@admin.route('/tv/saveImg',methods=['GET','POST'])
def tvSaveImg():
    name = request.args.get("name")
    if name == 'voteTrand':
        TvAnalysis().voteTrand()
        pass
    elif name == 'voteOfKindsTv':
        TvAnalysis().voteOfKindsTv()
        pass
    elif name == 'voteOfDirector':
        TvAnalysis().voteOfDirector()
        pass
    elif name == 'top10voteDirector':
        TvAnalysis().top10voteDirector()
        pass
    elif name == 'voteOfActors':
        TvAnalysis().voteOfActors()
        pass
    return "success"


@admin.route('/tv/predict',methods=['GET','POST'])
def tvPredict():
    return render_template('tvpredict.html')


@admin.route('/tv/recommond',methods=['GET','POST'])
def tvRecommond():
    return render_template('tvrecommond.html')


@admin.route('/tv/table/total',methods=['GET','POST'])
def tvTableTotal():
    id = request.args.get("id")
    content = request.args.get("searchContent")
    sql = "select count(*) from fatia_tv_copy1 WHERE "
    print(id == '')
    if content != '':
        if id == '':
            sql = "select count(*) from fatia_tv_copy1"
        elif int(id) == 1:
            sql += "title like '%" + content + "%'"
        elif int(id) == 2:
            sql += "derector like  '%" + content + "%'"
        elif int(id) == 3:
            sql += "actors like '%" + content + "%'"
        elif int(id) == 4:
            sql += "release_time = '" + content + "'"
        else:
            sql = "select count(*) from fatia_tv_copy1"
    else:
        sql = "select count(*) from fatia_tv_copy1"
    conn, cur = DBConnection.getDBConnection()
    cur.execute(sql)
    return json.dumps(cur.fetchone())

@admin.route('tv/delete',methods=['GET','POST'])
def tvDelete():
    itemId = request.form['id']
    conn, cur = DBConnection.getDBConnection()
    sql = 'delete from fatia_tv_copy1 where id = %d' % int(itemId)
    number = cur.execute(sql)
    conn.commit()
    return "success"

def tvSpider():
    global isRunTv
    isRunTv = True
    TvSpider().start()

# tv/startSpider
@admin.route('/tv/startSpider',methods=['GET','POST'])
def tvStartSpider():
    thead_one = threading.Thread(target=tvSpider)
    thead_one.start()
    return "success"

#tv/getCountDateSpider
@admin.route('tv/getCountDateSpider',methods=['GET','POST'])
def tvGetCountDateSpider():
    conn, cur = DBConnection.getDBConnection()
    sql = 'select count(*) from fatia_tv_copy1'
    cur.execute(sql)
    count = cur.fetchone()
    print(count)
    count = count[0]
    return str(round((count / 170000.0) * 100, 2)) + '%'


@admin.route('tv/getIsRunSpirder',methods=['GET','POST'])
def tvGetIsRunSpider():
    if isRunTv:
        return "1"
    else:
        return "0"
    pass

#tv/forecast
@admin.route('tv/forecast',methods=['GET','POST'])
def tvForcast():
    tvName = request.args.get("tvName")
    tvType = request.args.get("tvType")
    tvTime = request.args.get("tvTime")
    director = request.args.get("director")
    actors = request.args.get("actors")
    area = request.args.get("area")
    company = request.args.get("company")
    dates = {
        "id":"-1",
        "title":tvName,
        "types":tvType,
        "time":tvTime,
        "director":director,
        "actors":actors,
        "area":area,
        "vote_average":"8.7",
        "hot":"10000"
    }
    vote = TvPredict().predict(dates)
    print(vote)
    return str(vote)

#tv/recommonded
@admin.route('tv/recommonded',methods=['GET','POST'])
def tvRecommonded():
    conn, cur = DBConnection.getDBConnection()
    type = request.args.get("type")
    type = type.lower()
    if type is None or len(type) == 0:
        sql = "select title,director,vote_average,url from fatia_tv_copy1 order by vote_average desc limit 0,20"
    else:
        sql = 'select title,director,vote_average,url from fatia_tv_copy1 where genres like "%s%s%s" order by vote_average desc limit 0,20' % (
        "%", type, "%")

    cur.execute(sql)
    return json.dumps(cur.fetchall())