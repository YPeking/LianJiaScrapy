#!/usr/bin/python3
#encoding:utf-8

import os
import sys
import pymysql

## 连接数据库
def PriceMeans():
    conn = pymysql.connect(host='127.0.0.1', unix_socket='/run/mysqld/mysqld.sock',\
        user='root', passwd='mysql密码', db = 'BeiJing', charset= "utf8")
    cur = conn.cursor()

    # 获取BingJing数据库中的表名称
    sql = 'use BeiJing;'
    cur.execute(sql)
    conn.commit()
    

    table_name = ['dongcheng', 'xicheng', 'chaoyang', 'haidian',\
        'fengtai', 'shijingshan', 'tongzhou', 'changping',\
        'daxing', 'yizhuangkaifaqu', 'shunyi', 'fangshan',\
        'mentougou', 'pinggu', 'huairou', 'miyun',\
        'yanqing', 'yanjiao']
    max_lat = 0
    min_lat = 200
    max_lng = 0
    min_lng = 200
    for name in table_name:
        # 更新最大纬度
        sql = 'select Latitude from ' + name + ' order by Latitude DESC limit 0,1;'
        cur.execute(sql)
        result = cur.fetchone()
        if result[0] > max_lat:
            max_lat = result[0]

        # 更新最小纬度
        sql = 'select Latitude from  ' + name + ' order by Latitude ASC limit 0,1 ;'
        cur.execute(sql)
        result = cur.fetchone()
        if (result[0] < min_lat) and (result[0] != 0):   
            min_lat = result[0]

        # 更新最大经度
        sql = 'select Longitude from ' + name + ' order by Longitude DESC limit 0,1;'
        cur.execute(sql)
        result = cur.fetchone()
        if result[0] > max_lng:
            max_lng = result[0]

        # 更新最小经度
        sql = 'select Longitude from ' + name + ' order by Longitude ASC limit 0,1;'
        cur.execute(sql)
        result = cur.fetchone()
        if (result[0] < min_lng) and (result[0] != 0):
            min_lng = result[0]
    # 划片,每0.5°作为一个单元网格
    step = 0.01
    lat_num = int((max_lat - min_lat) / step) + 1
    lng_num = int((max_lng - min_lng) / step) + 1
    TotalPriceList = [([0] * lng_num) for i in range(lat_num)]
    CellNumList = [([0] * lng_num) for i in range(lat_num)]

    # 从数据库中取出数据并进行加和，求单元网格内的房价平均值
    for name in table_name:
        sql = 'select  Latitude, Longitude, UnitPrice from ' + name + ';'
        cur.execute(sql)
        result = cur.fetchall()
        for i in range(len(result)):
            lat_index = int((result[i][0] - min_lat) / step)
            lng_index = int((result[i][1] - min_lng) / step)
            if((lat_index >= 0) and (lng_index >= 0)):
                TotalPriceList[lat_index][lng_index] = \
                        TotalPriceList[lat_index][lng_index] + result[i][2]
                CellNumList[lat_index][lng_index] = \
                        CellNumList[lat_index][lng_index] + 1
    
    # 输出热力图数据 
    output = open('./MeanPrice.txt', 'w+')
    for i in range(lat_num):
        for j in range(lng_num):
            # 单元网格内中心纬度／经度／平均房价
            center_lat = min_lat + (i + 0.5) * step
            center_lng = min_lng + (j + 0.5) * step
            if CellNumList[i][j] != 0:
                MeanPrice = TotalPriceList[i][j] / (CellNumList[i][j] * 1200)
                output.write('{\"lng\":%f,\"lat\":%f,\"count\":%f},\n' % \
                        (center_lng, center_lat, MeanPrice))
    output.close()

    # 关闭数据库
    cur.close()
    conn.close()
if __name__ == '__main__':
    PriceMeans()
