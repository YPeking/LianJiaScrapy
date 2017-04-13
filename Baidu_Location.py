#!/usr/bin/python3 
#encoding:utf-8

import urllib.request
import urllib.error
import json
import urllib.parse
import pymysql

def BaiduLocation():
##------------------------------------------------
    # 连接到数据库
    conn = pymysql.connect(host='127.0.0.1', unix_socket='/run/mysqld/mysqld.sock',\
            user='root', passwd='mysql密码', db = 'BeiJing', charset= "utf8")
    cur = conn.cursor()

    sql = 'use BeiJing;'
    cur.execute(sql)
    conn.commit()

    # 从百度API查询经纬度
    Area_name = ['dongcheng', 'xicheng', 'chaoyang', 'haidian',\
            'fengtai', 'shijingshan', 'tongzhou', 'changping',\
            'daxing', 'yizhuangkaifaqu', 'shunyi', 'fangshan',\
            'mentougou', 'pinggu', 'huairou', 'miyun',\
            'yanqing', 'yanjiao']

    # 遍历北京城区，并查询小区经纬度
    for area in Area_name:
        sql = 'select houseId, CellName from ' + area + ' where Latitude = 0;';
        cur.execute(sql)
        result = cur.fetchall()
        for i in range(len(result)):
            house_lat_lng = get_Location(result[i][1])
            if house_lat_lng  != None:
                sql = 'update ' + area + ' set Latitude = %s, Longitude= %s  where houseId = ' + str(result[i][0]) + ';'
                cur.execute(sql, house_lat_lng)
                conn.commit()
            else:
                cur.close()
                conn.close()
                exit()
    cur.close()
    conn.close()

def get_Location(address):
    key = "百度地图API key"
    addr = urllib.parse.quote(address)
    url = "http://api.map.baidu.com/geocoder/v2/?address=" + addr + "&output=json&ak=" + key + "&callback=showLocation"
    try:
        req = urllib.request.Request(url)
        response = urllib.request.urlopen(req)
        html = response.read().decode('utf-8')
        answer = json.loads(html[27:-1])
        house_location = []
        house_location.append(answer['result']['location']['lat'])
        house_location.append(answer['result']['location']['lng'])
        return house_location
    except:
        return None

if __name__ == '__main__':
    BaiduLocation()

